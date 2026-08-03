/*
Copyright 2019 The Kubernetes Authors.

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

package podtopologyspread

import (
	"context"
	"fmt"
	"maps"
	"math"
	"slices"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/component-helpers/resource"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
)

const preFilterStateKey = "PreFilter" + Name

// preFilterState computed at PreFilter and used at Filter.
// It combines MinMatchNum, DomainsAtCount and TpValueToMatchNum to represent:
// (1) critical paths where the least pods are matched on each spread constraint.
// (2) number of pods matched on each spread constraint.
// A nil preFilterState denotes it's not set at all (in PreFilter phase);
// An empty preFilterState object denotes it's a legit state and is set in PreFilter phase.
// Fields are exported for comparison during testing.
type preFilterState struct {
	Constraints []topologySpreadConstraint
	// MinMatchNum[i] is the global minimum number of matching pods across all
	// eligible topology domains of Constraints[i].
	MinMatchNum []int
	// DomainsAtCount[i][k] is how many topology domains currently hold exactly k
	// matching pods. It is what keeps MinMatchNum exact in O(1) under the ±1
	// updates performed by AddPod/RemovePod.
	DomainsAtCount []map[int]int
	TpValueToMatchNum []map[string]int
}

// minMatchNum returns the global minimum for the calculation of skew while taking MinDomains into account.
func (s *preFilterState) minMatchNum(constraintID int, minDomains int32) (int, error) {
	if len(s.TpValueToMatchNum[constraintID]) < int(minDomains) {
		// Fewer eligible domains than minDomains: treat the global minimum as 0.
		return 0, nil
	}
	return s.MinMatchNum[constraintID], nil
}

// Clone makes a copy of the given state.
func (s *preFilterState) Clone() fwk.StateData {
	if s == nil {
		return nil
	}
	copy := preFilterState{
		Constraints:       s.Constraints, // shared, immutable
		MinMatchNum:       slices.Clone(s.MinMatchNum),
		DomainsAtCount:    make([]map[int]int, len(s.DomainsAtCount)),
		TpValueToMatchNum: make([]map[string]int, len(s.TpValueToMatchNum)),
	}
	for i, h := range s.DomainsAtCount {
		copy.DomainsAtCount[i] = maps.Clone(h)
	}
	for i, m := range s.TpValueToMatchNum {
		copy.TpValueToMatchNum[i] = maps.Clone(m)
	}
	return &copy
}

// updateMin keeps MinMatchNum[constraintID] exact after a single domain moved from
// `old` to `cur`. It relies on |cur-old| == 1, which holds because AddPod/RemovePod
// apply exactly one pod at a time.
func (s *preFilterState) updateMin(constraintID, old, cur int) {
	hist := s.DomainsAtCount[constraintID]
	hist[old]--
	if hist[old] <= 0 {
		delete(hist, old)
	}
	hist[cur]++

	switch {
	case cur < s.MinMatchNum[constraintID]:
		// A domain dropped below the current minimum (delta == -1).
		s.MinMatchNum[constraintID] = cur
	case old == s.MinMatchNum[constraintID] && hist[old] == 0:
		// The last domain sitting at the minimum moved up (delta == +1). Every other
		// domain was already >= old, and this one is now old+1 == cur, so cur is the
		// new global minimum — nothing needs to be scanned.
		s.MinMatchNum[constraintID] = cur
	}
}

// PreFilter invoked at the prefilter extension point.
func (pl *PodTopologySpread) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	s, err := pl.calPreFilterState(ctx, pod, nodes)
	if err != nil {
		return nil, fwk.AsStatus(err)
	} else if s != nil && len(s.Constraints) == 0 {
		return nil, fwk.NewStatus(fwk.Skip)
	}

	cycleState.Write(preFilterStateKey, s)
	return nil, nil
}

// PreFilterExtensions returns prefilter extensions, pod add and remove.
func (pl *PodTopologySpread) PreFilterExtensions() fwk.PreFilterExtensions {
	return pl
}

// AddPod from pre-computed data in cycleState.
func (pl *PodTopologySpread) AddPod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToAdd fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx)

	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	pl.updateWithPod(logger, s, podInfoToAdd.GetPod(), podToSchedule, nodeInfo.Node(), 1)
	return nil
}

// RemovePod from pre-computed data in cycleState.
func (pl *PodTopologySpread) RemovePod(ctx context.Context, cycleState fwk.CycleState, podToSchedule *v1.Pod, podInfoToRemove fwk.PodInfo, nodeInfo fwk.NodeInfo) *fwk.Status {
	logger := klog.FromContext(ctx)
	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	pl.updateWithPod(logger, s, podInfoToRemove.GetPod(), podToSchedule, nodeInfo.Node(), -1)
	return nil
}

func (pl *PodTopologySpread) updateWithPod(logger klog.Logger, s *preFilterState, updatedPod, preemptorPod *v1.Pod, node *v1.Node, delta int) {
	if s == nil || updatedPod.Namespace != preemptorPod.Namespace || node == nil {
		return
	}
	if !nodeLabelsMatchSpreadConstraints(node.Labels, s.Constraints) {
		return
	}

	requiredSchedulingTerm := nodeaffinity.GetRequiredNodeAffinity(preemptorPod)
	if !pl.enableNodeInclusionPolicyInPodTopologySpread {
		// spreading is applied to nodes that pass those filters.
		// Ignore parsing errors for backwards compatibility.
		if match, _ := requiredSchedulingTerm.Match(node); !match {
			return
		}
	}

	podLabelSet := labels.Set(updatedPod.Labels)
	for i, constraint := range s.Constraints {
		if !constraint.Selector.Matches(podLabelSet) {
			continue
		}

		if pl.enableNodeInclusionPolicyInPodTopologySpread &&
			!constraint.matchNodeInclusionPolicies(logger, preemptorPod, node, requiredSchedulingTerm, pl.enableTaintTolerationComparisonOperators) {
			continue
		}

		v := node.Labels[constraint.TopologyKey]
		old := s.TpValueToMatchNum[i][v]
		cur := old + delta
		s.TpValueToMatchNum[i][v] = cur
		s.updateMin(i, old, cur)
	}
}

// getPreFilterState fetches a pre-computed preFilterState.
func getPreFilterState(cycleState fwk.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// preFilterState doesn't exist, likely PreFilter wasn't invoked.
		return nil, fmt.Errorf("reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to podtopologyspread.preFilterState error", c)
	}
	return s, nil
}

type topologyCount struct {
	topologyValue string
	constraintID  int
	count         int
}

// calPreFilterState computes preFilterState describing how pods are spread on topologies.
func (pl *PodTopologySpread) calPreFilterState(ctx context.Context, pod *v1.Pod, allNodes []fwk.NodeInfo) (*preFilterState, error) {
	constraints, err := pl.getConstraints(pod)
	if err != nil {
		return nil, fmt.Errorf("get constraints from pod: %w", err)
	}
	if len(constraints) == 0 {
		return &preFilterState{}, nil
	}

	logger := klog.FromContext(ctx)
	s := preFilterState{
		Constraints:       constraints,
		MinMatchNum:       make([]int, len(constraints)),
		DomainsAtCount:    make([]map[int]int, len(constraints)),
		TpValueToMatchNum: make([]map[string]int, len(constraints)),
	}
	for i := 0; i < len(constraints); i++ {
		s.TpValueToMatchNum[i] = make(map[string]int, sizeHeuristic(len(allNodes), constraints[i]))
	}

	tpCountsByNode := make([][]topologyCount, len(allNodes))
	requiredNodeAffinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	processNode := func(n int) {
		nodeInfo := allNodes[n]
		node := nodeInfo.Node()

		if !pl.enableNodeInclusionPolicyInPodTopologySpread {
			// spreading is applied to nodes that pass those filters.
			// Ignore parsing errors for backwards compatibility.
			if match, _ := requiredNodeAffinity.Match(node); !match {
				return
			}
		}

		// Ensure current node's labels contains all topologyKeys in 'Constraints'.
		if !nodeLabelsMatchSpreadConstraints(node.Labels, constraints) {
			return
		}

		tpCounts := make([]topologyCount, 0, len(constraints))
		for i, c := range constraints {
			if pl.enableNodeInclusionPolicyInPodTopologySpread &&
				!c.matchNodeInclusionPolicies(logger, pod, node, requiredNodeAffinity, pl.enableTaintTolerationComparisonOperators) {
				continue
			}

			value := node.Labels[c.TopologyKey]
			count := countPodsMatchSelector(nodeInfo.GetPods(), c.Selector, pod.Namespace)
			tpCounts = append(tpCounts, topologyCount{
				topologyValue: value,
				constraintID:  i,
				count:         count,
			})
		}
		tpCountsByNode[n] = tpCounts
	}
	pl.parallelizer.Until(ctx, len(allNodes), processNode, pl.Name())

	for _, tpCounts := range tpCountsByNode {
		// tpCounts might not hold all the constraints, so index can't be used here as constraintID.
		for _, tpCount := range tpCounts {
			s.TpValueToMatchNum[tpCount.constraintID][tpCount.topologyValue] += tpCount.count
		}
	}

	for i := 0; i < len(constraints); i++ {
		s.MinMatchNum[i] = math.MaxInt32 // preserves today's behaviour when no domain is eligible
		s.DomainsAtCount[i] = make(map[int]int)
		for _, num := range s.TpValueToMatchNum[i] {
			s.DomainsAtCount[i][num]++
			if num < s.MinMatchNum[i] {
				s.MinMatchNum[i] = num
			}
		}
	}

	return &s, nil
}

// Filter invoked at the filter extension point.
func (pl *PodTopologySpread) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	node := nodeInfo.Node()

	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	// However, "empty" preFilterState is legit which tolerates every toSchedule Pod.
	if len(s.Constraints) == 0 {
		return nil
	}

	logger := klog.FromContext(ctx)
	podLabelSet := labels.Set(pod.Labels)
	for i, c := range s.Constraints {
		tpKey := c.TopologyKey
		tpVal, ok := node.Labels[tpKey]
		if !ok {
			logger.V(5).Info("Node doesn't have required topology label for spread constraint", "node", klog.KObj(node), "topologyKey", tpKey)
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, ErrReasonNodeLabelNotMatch)
		}

		// judging criteria:
		// 'existing matching num' + 'if self-match (1 or 0)' - 'global minimum' <= 'maxSkew'
		minMatchNum, err := s.minMatchNum(i, c.MinDomains)
		if err != nil {
			logger.Error(err, "Internal error occurred while retrieving value precalculated in PreFilter", "topologyKey", tpKey, "minMatchNum", s.MinMatchNum[i])
			continue
		}

		selfMatchNum := 0
		if c.Selector.Matches(podLabelSet) {
			selfMatchNum = 1
		}

		matchNum := s.TpValueToMatchNum[i][tpVal]
		skew := matchNum + selfMatchNum - minMatchNum
		if skew > int(c.MaxSkew) {
			logger.V(5).Info("Node failed spreadConstraint: matchNum + selfMatchNum - minMatchNum > maxSkew", "node", klog.KObj(node), "topologyKey", tpKey, "matchNum", matchNum, "selfMatchNum", selfMatchNum, "minMatchNum", minMatchNum, "maxSkew", c.MaxSkew)
			return fwk.NewStatus(fwk.Unschedulable, ErrReasonConstraintsNotMatch)
		}
	}

	return nil
}

func sizeHeuristic(nodes int, constraint topologySpreadConstraint) int {
	if constraint.TopologyKey == v1.LabelHostname {
		return nodes
	}
	return 0
}
