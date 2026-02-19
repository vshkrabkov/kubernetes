/*
Copyright 2025 The Kubernetes Authors.

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

package helper

import (
	"context"
	"fmt"
	"time"

	schedulingapi "k8s.io/api/scheduling/v1alpha1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"

	v1 "k8s.io/api/core/v1"
)

// PodGroupPolicyEvaluator centralizes the enforcement of scheduling policies defined
// within a Workload object (e.g., Gang or Basic policies).
//
// Its primary purpose is to ensure consistent behavior across different framework
// extension points (PreEnqueue, Permit) and multiple plugins (Coscheduling, GangScheduling).
// By centralizing this logic, the evaluator maintains invariant symmetry: the conditions
// that cause a pod to be rejected in PreEnqueue are the same conditions evaluated by
// the QueueingHints to determine when a pod should be retried.
//
// This struct is designed to be stateless regarding cluster data; it relies on
// caller-provided counts or the framework handle to access the most recent cache state.
type PodGroupPolicyEvaluator struct {
	enablePodGroupDesiredCount bool
}

func New(enablePodGroupDesiredCount bool) *PodGroupPolicyEvaluator {
	return &PodGroupPolicyEvaluator{
		enablePodGroupDesiredCount: enablePodGroupDesiredCount,
	}
}

// matchingWorkloadReference returns true if two pods belong to the same workload, including their pod group and replica key.
func matchingWorkloadReference(pod1, pod2 *v1.Pod) bool {
	return pod1.Spec.WorkloadRef != nil && pod2.Spec.WorkloadRef != nil && pod1.Namespace == pod2.Namespace && *pod1.Spec.WorkloadRef == *pod2.Spec.WorkloadRef
}

func (e *PodGroupPolicyEvaluator) IsReEvaluatingNeedAfterPodAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, addedPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if !matchingWorkloadReference(pod, addedPod) {
		logger.V(5).Info("another pod was added but it doesn't match the target pod's workload",
			"pod", klog.KObj(pod), "workloadRef", pod.Spec.WorkloadRef, "addedPod", klog.KObj(addedPod), "addedWorkloadRef", pod.Spec.WorkloadRef)
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("another pod was added and it matches the target pod's workload, which may make the pod schedulable",
		"pod", klog.KObj(pod), "workloadRef", pod.Spec.WorkloadRef, "addedPod", klog.KObj(addedPod), "addedWorkloadRef", pod.Spec.WorkloadRef)
	return fwk.Queue, nil
}

func (e *PodGroupPolicyEvaluator) IsReEvaluatingNeedAfterWorkloadAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, addedWorkload, err := util.As[*schedulingapi.Workload](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if pod.Spec.WorkloadRef == nil || pod.Namespace != addedWorkload.Namespace || pod.Spec.WorkloadRef.Name != addedWorkload.Name {
		logger.V(5).Info("workload was added but it doesn't match the target pod's workloadRef",
			"pod", klog.KObj(pod), "workloadRef", pod.Spec.WorkloadRef, "addedWorkload", klog.KObj(addedWorkload))
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("workload was added and it matches the target pod's workload, which may make the pod schedulable",
		"pod", klog.KObj(pod), "workloadRef", pod.Spec.WorkloadRef, "addedWorkload", klog.KObj(addedWorkload))
	return fwk.Queue, nil
}

func (e *PodGroupPolicyEvaluator) PreEnqueue(pod *v1.Pod, policy schedulingapi.PodGroupPolicy, allPodsCount int) *fwk.Status {
	if policy.Gang != nil && allPodsCount < int(policy.Gang.MinCount) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "waiting for minCount pods from a gang to appear in scheduling queue")
	}

	var desiredCount *int32
	if policy.Basic != nil {
		desiredCount = policy.Basic.DesiredCount
	} else if policy.Gang != nil {
		desiredCount = policy.Gang.DesiredCount
	}

	if e.enablePodGroupDesiredCount && desiredCount != nil && allPodsCount < int(*desiredCount) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("introducing delay while all pods count: %d doesn't satisfy desired count requirement: %d", allPodsCount, *desiredCount))
	}

	return nil
}

func (e *PodGroupPolicyEvaluator) Permit(handle fwk.Handle, ctx context.Context, pod *v1.Pod, podGroupState fwk.PodGroupState, policy schedulingapi.PodGroupPolicy) (*fwk.Status, time.Duration) {
	if policy.Gang == nil {
		return nil, 0
	}

	logger := klog.FromContext(ctx)

	assumedPods := podGroupState.AssumedPods()
	assumedOrAssignedPods := assumedPods.Union(podGroupState.AssignedPods())
	if len(assumedOrAssignedPods) < int(policy.Gang.MinCount) {
		// Activate unscheduled pods from this pod group in case they were waiting for this pod to be scheduled.
		unscheduledPods := podGroupState.UnscheduledPods()
		handle.Activate(klog.FromContext(ctx), unscheduledPods)
		logger.V(4).Info("Quorum is not met for a gang. Waiting for another pod to allow", "pod", klog.KObj(pod), "workloadRef", pod.Spec.WorkloadRef, "activatedPods", len(unscheduledPods))
		return fwk.NewStatus(fwk.Wait, "waiting for minCount pods from a gang to be scheduled"), podGroupState.SchedulingTimeout()
	}

	return nil, 0
}

// podGroupPolicy is a helper to find the policy for a specific pod group name in a workload.
func PodGroupPolicy(workload *schedulingapi.Workload, podGroupName string) (schedulingapi.PodGroupPolicy, bool) {
	for _, podGroup := range workload.Spec.PodGroups {
		if podGroup.Name == podGroupName {
			return podGroup.Policy, true
		}
	}
	return schedulingapi.PodGroupPolicy{}, false
}
