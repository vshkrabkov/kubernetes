/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	utiltrace "k8s.io/utils/trace"
)

// SchedulingAlgorithm holds the in-memory part of a scheduling attempt: picking a
// node for a pod and recording the resulting placement in the scheduler's stores.
// It owns the state that used to live on Scheduler for those steps, so the two
// concerns can evolve separately.
type SchedulingAlgorithm struct {
	nodeInfoSnapshot         *internalcache.Snapshot
	cache                    internalcache.Cache
	percentageOfNodesToScore int32
	cycleProvider            func() int64
	nextStartNodeIndex       int
}

func (a *SchedulingAlgorithm) currentCycle() int64 {
	return a.cycleProvider()
}

type AlgorithmOption func(*SchedulingAlgorithm)

// withPercentageOfNodesToScore is unexported because the exported name is taken by
// the scheduler.Option serving the same purpose, and Go has no
// overloading. kube-scheduler wiring lives in this package, so unexported is enough.
func withPercentageOfNodesToScore(percentage int32) AlgorithmOption {
	return func(a *SchedulingAlgorithm) {
		a.percentageOfNodesToScore = percentage
	}
}

// NewSchedulingAlgorithm creates a scheduling algorithm operating on the given
// snapshot and cache. cycleProvider reports the scheduling cycle the algorithm is
// running in.
func NewSchedulingAlgorithm(snapshot *internalcache.Snapshot, cache internalcache.Cache,
	cycleProvider func() int64, opts ...AlgorithmOption) *SchedulingAlgorithm {
	a := &SchedulingAlgorithm{
		nodeInfoSnapshot:         snapshot,
		cache:                    cache,
		cycleProvider:            cycleProvider,
		percentageOfNodesToScore: schedulerapi.DefaultPercentageOfNodesToScore,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// schedulePod tries to schedule the given pod to one of the nodes in the node list.
// If it succeeds, it will return the name of the node.
// If it fails, it will return a FitError with reasons.
func (a *SchedulingAlgorithm) schedulePod(ctx context.Context, fwk framework.Framework, state fwk.CycleState, podInfo *framework.QueuedPodInfo) (result ScheduleResult, err error) {
	pod := podInfo.Pod
	trace := utiltrace.New("Scheduling", utiltrace.Field{Key: "namespace", Value: pod.Namespace}, utiltrace.Field{Key: "name", Value: pod.Name})
	defer trace.LogIfLong(100 * time.Millisecond)

	if a.nodeInfoSnapshot.NumNodesInPlacement() == 0 {
		return result, ErrNoNodesAvailable
	}

	feasibleNodes, diagnosis, nodeHint, err := a.findNodesThatFitPod(ctx, fwk, state, podInfo)
	if err != nil {
		return result, err
	}
	trace.Step("Computing predicates done")

	if len(feasibleNodes) == 0 {
		return result, &framework.FitError{
			Pod:         pod,
			NumAllNodes: a.nodeInfoSnapshot.NumNodesInPlacement(),
			Diagnosis:   diagnosis,
		}
	}

	// When only one node after predicate, just use it.
	if len(feasibleNodes) == 1 {
		node := feasibleNodes[0].Node().Name
		if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
			fwk.StoreScheduleResults(ctx, podInfo.PodSignature, nodeHint, node, nil, a.currentCycle())
		}
		return ScheduleResult{
			SuggestedHost:  node,
			EvaluatedNodes: 1 + diagnosis.NodeToStatus.Len(),
			FeasibleNodes:  1,
		}, nil
	}

	priorityList, err := prioritizeNodes(ctx, fwk, state, pod, feasibleNodes)
	if err != nil {
		return result, err
	}

	sortedPrioritizedNodes := framework.NewSortedScoredNodes(priorityList)
	node := sortedPrioritizedNodes.Pop().Name
	trace.Step("Prioritizing done")

	if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
		fwk.StoreScheduleResults(ctx, podInfo.PodSignature, nodeHint, node, sortedPrioritizedNodes, a.currentCycle())
	}

	return ScheduleResult{
		SuggestedHost:  node,
		EvaluatedNodes: len(feasibleNodes) + diagnosis.NodeToStatus.Len(),
		FeasibleNodes:  len(feasibleNodes),
	}, err
}

// Filters the nodes to find the ones that fit the pod based on the framework
// filter plugins and filter extenders.
func (a *SchedulingAlgorithm) findNodesThatFitPod(ctx context.Context, schedFramework framework.Framework, state fwk.CycleState, podInfo *framework.QueuedPodInfo) ([]fwk.NodeInfo, framework.Diagnosis, string, error) {
	logger := klog.FromContext(ctx)
	diagnosis := framework.Diagnosis{
		NodeToStatus: framework.NewDefaultNodeToStatus(),
	}
	allNodes, err := a.nodeInfoSnapshot.ListNodesInPlacement()
	if err != nil {
		return nil, diagnosis, "", err
	}
	// Run "prefilter" plugins.
	pod := podInfo.Pod
	preRes, s, unscheduledPlugins := schedFramework.RunPreFilterPlugins(ctx, state, pod)
	diagnosis.UnschedulablePlugins = unscheduledPlugins
	if !s.IsSuccess() {
		if !s.IsRejected() {
			return nil, diagnosis, "", s.AsError()
		}
		// All nodes in NodeToStatus will have the same status so that they can be handled in the preemption.
		diagnosis.NodeToStatus.SetAbsentNodesStatus(s)

		// Record the messages from PreFilter in Diagnosis.PreFilterMsg.
		msg := s.Message()
		diagnosis.PreFilterMsg = msg
		logger.V(5).Info("Status after running PreFilter plugins for pod", "pod", klog.KObj(pod), "status", msg)
		diagnosis.AddPluginStatus(s)
		return nil, diagnosis, "", nil
	}

	var nodeHint string
	if utilfeature.DefaultFeatureGate.Enabled(features.OpportunisticBatching) {
		// We get the node hint even if we have a nominated name for simplicity, but we could potentially avoid it
		// in this scenario in the future.
		nodeHint = schedFramework.GetNodeHint(ctx, pod, podInfo.PodSignature, state, a.currentCycle())
	}

	// "NominatedNodeName" can potentially be set in a previous scheduling cycle as a result of preemption.
	// This node is likely the only candidate that will fit the pod, and hence we try it first before iterating over all nodes.
	// We take the same tack for hinted nodes from the batch module.
	if len(pod.Status.NominatedNodeName) > 0 || len(nodeHint) > 0 {
		feasibleNodes, err := a.evaluateNominatedNode(ctx, pod, schedFramework, state, nodeHint, diagnosis)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Evaluation failed on nominated node", "pod", klog.KObj(pod), "node", pod.Status.NominatedNodeName)
		}
		// Nominated node passes all the filters, scheduler is good to assign this node to the pod.
		if len(feasibleNodes) != 0 {
			return feasibleNodes, diagnosis, nodeHint, nil
		}
	}

	nodes := allNodes
	if !preRes.AllNodes() {
		nodes = make([]fwk.NodeInfo, 0, len(preRes.NodeNames))
		for nodeName := range preRes.NodeNames {
			// PreRes may return nodeName(s) which do not exist; we verify
			// node exists in the Snapshot within the selected placement.
			if nodeInfo, err := a.nodeInfoSnapshot.GetNodeInPlacement(nodeName); err == nil {
				nodes = append(nodes, nodeInfo)
			}
		}
		diagnosis.NodeToStatus.SetAbsentNodesStatus(fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("node(s) didn't satisfy plugin(s) %v", sets.List(unscheduledPlugins))))
	}
	feasibleNodes, err := a.findNodesThatPassFilters(ctx, schedFramework, state, pod, &diagnosis, nodes)
	// always try to update the a.nextStartNodeIndex regardless of whether an error has occurred
	// this is helpful to make sure that all the nodes have a chance to be searched
	processedNodes := len(feasibleNodes) + diagnosis.NodeToStatus.Len()
	a.nextStartNodeIndex = (a.nextStartNodeIndex + processedNodes) % len(allNodes)
	if err != nil {
		return nil, diagnosis, nodeHint, err
	}

	feasibleNodesAfterExtender, err := findNodesThatPassExtenders(ctx, schedFramework.Extenders(), pod, feasibleNodes, diagnosis.NodeToStatus)
	if err != nil {
		return nil, diagnosis, nodeHint, err
	}
	if len(feasibleNodesAfterExtender) != len(feasibleNodes) {
		// Extenders filtered out some nodes.
		//
		// Extender doesn't support any kind of requeueing feature like EnqueueExtensions in the scheduling framework.
		// When Extenders reject some Nodes and the pod ends up being unschedulable,
		// we put fwk.ExtenderName to pInfo.UnschedulablePlugins.
		// This Pod will be requeued from unschedulable pod pool to activeQ/backoffQ
		// by any kind of cluster events.
		// https://github.com/kubernetes/kubernetes/issues/122019
		if diagnosis.UnschedulablePlugins == nil {
			diagnosis.UnschedulablePlugins = sets.New[string]()
		}
		diagnosis.UnschedulablePlugins.Insert(framework.ExtenderName)
	}

	return feasibleNodesAfterExtender, diagnosis, nodeHint, nil
}

func (a *SchedulingAlgorithm) evaluateNominatedNode(ctx context.Context, pod *v1.Pod, schedFramework framework.Framework, state fwk.CycleState, nodeHint string, diagnosis framework.Diagnosis) ([]fwk.NodeInfo, error) {
	// In the future we could potentially use the hint if the nominated node failed.
	// https://github.com/kubernetes/kubernetes/issues/135163
	nnn := pod.Status.NominatedNodeName
	if len(nnn) == 0 {
		nnn = nodeHint
	}

	nodeInfo, err := a.nodeInfoSnapshot.GetNodeInPlacement(nnn)
	if err != nil {
		if _, err := a.nodeInfoSnapshot.Get(nnn); err != nil {
			return nil, err
		}
		// It's not an error if NNN is in the cluster but not in the placement.
		// This can happen during the pod group placement scheduling cycle, where we simulate multiple potential placements.
		logger := klog.FromContext(ctx)
		logger.V(4).Info("Pod's nominated node is present in the cluster but not available in the current placement", "pod", klog.KObj(pod), "node", pod.Status.NominatedNodeName)
		return nil, nil
	}
	node := []fwk.NodeInfo{nodeInfo}
	feasibleNodes, err := a.findNodesThatPassFilters(ctx, schedFramework, state, pod, &diagnosis, node)
	if err != nil {
		return nil, err
	}

	feasibleNodes, err = findNodesThatPassExtenders(ctx, schedFramework.Extenders(), pod, feasibleNodes, diagnosis.NodeToStatus)
	if err != nil {
		return nil, err
	}

	return feasibleNodes, nil
}

// findNodesThatPassFilters finds the nodes that fit the filter plugins.
func (a *SchedulingAlgorithm) findNodesThatPassFilters(
	ctx context.Context,
	schedFramework framework.Framework,
	state fwk.CycleState,
	pod *v1.Pod,
	diagnosis *framework.Diagnosis,
	nodes []fwk.NodeInfo) ([]fwk.NodeInfo, error) {
	numAllNodes := len(nodes)
	numNodesToFind := a.numFeasibleNodesToFind(schedFramework.PercentageOfNodesToScore(), int32(numAllNodes))
	if !hasExtenderFilters(schedFramework) && !hasScoring(schedFramework) {
		numNodesToFind = 1
	}

	// Create feasible list with enough space to avoid growing it
	// and allow assigning.
	feasibleNodes := make([]fwk.NodeInfo, numNodesToFind)

	if !schedFramework.HasFilterPlugins() {
		for i := range feasibleNodes {
			feasibleNodes[i] = nodes[(a.nextStartNodeIndex+i)%numAllNodes]
		}
		return feasibleNodes, nil
	}

	errCh := parallelize.NewResultChannel[error]()
	var feasibleNodesLen int32
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("findNodesThatPassFilters has completed"))

	type nodeStatus struct {
		node   string
		status *fwk.Status
	}
	result := make([]*nodeStatus, numAllNodes)
	checkNode := func(i int) {
		// We check the nodes starting from where we left off in the previous scheduling cycle,
		// this is to make sure all nodes have the same chance of being examined across pods.
		nodeInfo := nodes[(a.nextStartNodeIndex+i)%numAllNodes]
		status := schedFramework.RunFilterPluginsWithNominatedPods(ctx, state, pod, nodeInfo)
		if status.Code() == fwk.Error {
			errCh.SendWithCancel(status.AsError(), func() {
				cancel(errors.New("some other Filter operation failed"))
			})
			return
		}
		if status.IsSuccess() {
			length := atomic.AddInt32(&feasibleNodesLen, 1)
			if length > numNodesToFind {
				cancel(errors.New("findNodesThatPassFilters has found enough nodes"))
				atomic.AddInt32(&feasibleNodesLen, -1)
			} else {
				feasibleNodes[length-1] = nodeInfo
			}
		} else {
			result[i] = &nodeStatus{node: nodeInfo.Node().Name, status: status}
		}
	}

	beginCheckNode := time.Now()
	statusCode := fwk.Success
	defer func() {
		// We record Filter extension point latency here instead of in framework.go because framework.RunFilterPlugins
		// function is called for each node, whereas we want to have an overall latency for all nodes per scheduling cycle.
		// Note that this latency also includes latency for `addNominatedPods`, which calls framework.RunPreFilterAddPod.
		metrics.FrameworkExtensionPointDuration.WithLabelValues(metrics.Filter, statusCode.String(), schedFramework.ProfileName()).Observe(metrics.SinceInSeconds(beginCheckNode))
	}()

	// Stops searching for more nodes once the configured number of feasible nodes
	// are found.
	schedFramework.Parallelizer().Until(ctx, numAllNodes, checkNode, metrics.Filter)
	feasibleNodes = feasibleNodes[:feasibleNodesLen]
	for _, item := range result {
		if item == nil {
			continue
		}
		diagnosis.NodeToStatus.Set(item.node, item.status)
		diagnosis.AddPluginStatus(item.status)
	}
	if err := errCh.Receive(); err != nil {
		statusCode = fwk.Error
		return feasibleNodes, err
	}
	return feasibleNodes, nil
}

// numFeasibleNodesToFind returns the number of feasible nodes that once found, the scheduler stops
// its search for more feasible nodes.
func (a *SchedulingAlgorithm) numFeasibleNodesToFind(percentageOfNodesToScore *int32, numAllNodes int32) (numNodes int32) {
	if numAllNodes < minFeasibleNodesToFind {
		return numAllNodes
	}

	// Use profile percentageOfNodesToScore if it's set. Otherwise, use global percentageOfNodesToScore.
	var percentage int32
	if percentageOfNodesToScore != nil {
		percentage = *percentageOfNodesToScore
	} else {
		percentage = a.percentageOfNodesToScore
	}

	if percentage == 0 {
		percentage = int32(50) - numAllNodes/125
		if percentage < minFeasibleNodesPercentageToFind {
			percentage = minFeasibleNodesPercentageToFind
		}
	}

	numNodes = numAllNodes * percentage / 100
	if numNodes < minFeasibleNodesToFind {
		return minFeasibleNodesToFind
	}

	return numNodes
}

// assume signals to the cache that a pod is already in the cache, so that binding can be asynchronous.
// When called during pod group scheduling cycle, pod is assumed in the snapshot instead.
func (a *SchedulingAlgorithm) assume(logger klog.Logger, schedFramework framework.Framework, state fwk.CycleState, assumedPodInfo *framework.QueuedPodInfo, host string) error {
	// Optimistically assume that the binding will succeed and send it to apiserver
	// in the background.
	// If the binding fails, scheduler will release resources allocated to assumed pod
	// immediately.
	assumedPodInfo.Pod.Spec.NodeName = host
	if utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources) {
		// If DRANodeAllocatableResources is enabled, copy the calculated node allocatable resource claim status
		// from the cycle state to the assumed pod's status. This ensures that the scheduler's
		// cached version of the pod reflects the node allocatable resources allocated by the DRA plugin
		// for this scheduling cycle, making this information available for NodeInfo cache update.
		// Any potential NodeAllocatableResourceClaimStatuses from a previously failed scheduling attempt is overwritten.
		// This field is not explicitly cleared as the Pod object is reconstructed in handleSchedulingFailure()
		// before re-queueing.
		assumedPodInfo.Pod.Status.NodeAllocatableResourceClaimStatuses = dynamicresources.ExtractPodNodeAllocatableResourceClaimStatus(logger, state, host)
	}

	if state.IsPodGroupSchedulingCycle() {
		err := a.nodeInfoSnapshot.AssumePod(assumedPodInfo.PodInfo)
		if err != nil {
			logger.Error(err, "Scheduler snapshot AssumePod failed")
			return err
		}
	} else {
		if err := a.cache.AssumePod(logger, assumedPodInfo.Pod); err != nil {
			logger.Error(err, "Scheduler cache AssumePod failed")
			return err
		}
	}
	// if "assumed" is a nominated pod, we should remove it from internal cache
	schedFramework.DeleteNominatedPodIfExists(assumedPodInfo.Pod)

	return nil
}

// assumeAndReserve assumes and reserves the pod in scheduler's memory.
func (a *SchedulingAlgorithm) assumeAndReserve(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	scheduleResult ScheduleResult,
) (*framework.QueuedPodInfo, *fwk.Status) {
	logger := klog.FromContext(ctx)
	// Tell the cache to assume that a pod now is running on a given node, even though it hasn't been bound yet.
	// This allows us to keep scheduling without waiting on binding to occur.
	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod
	// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
	err := a.assume(logger, schedFramework, state, assumedPodInfo, scheduleResult.SuggestedHost)
	if err != nil {
		// This is most probably result of a BUG in retrying logic.
		// We report an error here so that pod scheduling can be retried.
		// This relies on the fact that Error will check if the pod has been bound
		// to a node and if so will not add it back to the unscheduled pods queue
		// (otherwise this would cause an infinite loop).
		return assumedPodInfo, fwk.AsStatus(err)
	}

	// Run the Reserve method of reserve plugins.
	if sts := schedFramework.RunReservePluginsReserve(ctx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
		// trigger un-reserve to clean up state associated with the reserved Pod
		err := a.unreserveAndForget(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "ForgetPod failed")
		}

		if sts.IsRejected() {
			fitErr := &framework.FitError{
				NumAllNodes: 1,
				Pod:         podInfo.Pod,
				Diagnosis: framework.Diagnosis{
					NodeToStatus: framework.NewDefaultNodeToStatus(),
				},
			}
			fitErr.Diagnosis.NodeToStatus.Set(scheduleResult.SuggestedHost, sts)
			fitErr.Diagnosis.AddPluginStatus(sts)
			return assumedPodInfo, fwk.NewStatus(sts.Code()).WithError(fitErr)
		}
		return assumedPodInfo, sts
	}
	return assumedPodInfo, nil
}

// unreserveAndForget unreserves and forgets the pod from scheduler's memory.
// This function shouldn't be called during binding cycle with a state, where IsPodGroupSchedulingCycle is set to true,
// but this shouldn't happen, because such pods with such state cannot reach binding.
func (a *SchedulingAlgorithm) unreserveAndForget(
	ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	assumedPodInfo *framework.QueuedPodInfo,
	nodeName string,
) error {
	logger := klog.FromContext(ctx)

	schedFramework.RunReservePluginsUnreserve(ctx, state, assumedPodInfo.Pod, nodeName)
	if state.IsPodGroupSchedulingCycle() {
		err := a.nodeInfoSnapshot.ForgetPod(logger, assumedPodInfo.Pod)
		if err != nil {
			return err
		}
		if assumedPodInfo.Pod.Status.NominatedNodeName != "" {
			// Assume method removed the nomination, but since we are reverting that stage for pod groups,
			// we need to revert that operation as well.
			nominatingInfo := &fwk.NominatingInfo{
				NominatedNodeName: assumedPodInfo.Pod.Status.NominatedNodeName,
				NominatingMode:    fwk.ModeOverride,
			}
			// AssumedPodInfo can be used here, because the whole pod object is not stored in the nominator.
			schedFramework.AddNominatedPod(logger, assumedPodInfo.PodInfo, nominatingInfo)
		}
		return nil
	}
	return a.cache.ForgetPod(logger, assumedPodInfo.Pod)
}

// assumeAndReserveWithRevert assumes and reserves the pod, returning a function that
// reverts both steps. It is used by pod group scheduling, where a placement stays
// tentative until the whole group is submitted.
func (a *SchedulingAlgorithm) assumeAndReserveWithRevert(ctx context.Context,
	state fwk.CycleState,
	schedFramework framework.Framework,
	podInfo *framework.QueuedPodInfo,
	scheduleResult ScheduleResult,
) (*fwk.Status, func()) {
	assumedPodInfo, assumeStatus := a.assumeAndReserve(ctx, state, schedFramework, podInfo, scheduleResult)
	if !assumeStatus.IsSuccess() {
		return assumeStatus, nil
	}
	return assumeStatus, func() {
		err := a.unreserveAndForget(ctx, state, schedFramework, assumedPodInfo, scheduleResult.SuggestedHost)
		if err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "ForgetPod failed")
		}
	}
}
