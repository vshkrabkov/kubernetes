/*
Copyright 2026 The Kubernetes Authors.

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

package cache

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// nodeDump is a flattened, comparable image of one NodeInfo, including slice
// order and generation, used to verify that a mutation-session restore is exact.
type nodeDump struct {
	Name             string
	Pods             []string
	PodsWithAffinity []string
	PodsWithAntiAff  []string
	MilliCPU         int64
	Memory           int64
	PVCRefCounts     map[string]int
	Generation       int64
}

// snapshotDump is a flattened, comparable image of the whole snapshot.
type snapshotDump struct {
	Nodes            map[string]nodeDump
	NodeInfoList     []string
	AffinityList     []string
	AntiAffinityList []string
	UsedPVCRefCounts map[string]int
	PodGroups        map[string]podGroupDump
}

type podGroupDump struct {
	All         []string
	Unscheduled []string
	Assumed     []string
	Assigned    []string
	Generation  int64
}

func dumpSnapshot(s *Snapshot) *snapshotDump {
	d := &snapshotDump{
		Nodes:            map[string]nodeDump{},
		UsedPVCRefCounts: map[string]int{},
		PodGroups:        map[string]podGroupDump{},
	}
	for name, ni := range s.nodeInfoMap {
		nd := nodeDump{
			Name:         name,
			MilliCPU:     ni.Requested.MilliCPU,
			Memory:       ni.Requested.Memory,
			PVCRefCounts: map[string]int{},
			Generation:   ni.Generation,
		}
		for _, p := range ni.Pods {
			nd.Pods = append(nd.Pods, p.GetPod().Name)
		}
		for _, p := range ni.PodsWithAffinity {
			nd.PodsWithAffinity = append(nd.PodsWithAffinity, p.GetPod().Name)
		}
		for _, p := range ni.PodsWithRequiredAntiAffinity {
			nd.PodsWithAntiAff = append(nd.PodsWithAntiAff, p.GetPod().Name)
		}
		for k, v := range ni.PVCRefCounts {
			nd.PVCRefCounts[k] = v
		}
		d.Nodes[name] = nd
	}
	for _, ni := range s.nodeInfoList {
		d.NodeInfoList = append(d.NodeInfoList, ni.Node().Name)
	}
	for _, ni := range s.havePodsWithAffinityNodeInfoList {
		d.AffinityList = append(d.AffinityList, ni.Node().Name)
	}
	for _, ni := range s.havePodsWithRequiredAntiAffinityNodeInfoList {
		d.AntiAffinityList = append(d.AntiAffinityList, ni.Node().Name)
	}
	for k, v := range s.usedPVCRefCounts {
		d.UsedPVCRefCounts[k] = v
	}
	for key, pgs := range s.podGroupStates {
		pd := podGroupDump{Generation: pgs.generation}
		for uid := range pgs.allPods {
			pd.All = append(pd.All, string(uid))
		}
		for _, uid := range pgs.unscheduledPods.UnsortedList() {
			pd.Unscheduled = append(pd.Unscheduled, string(uid))
		}
		for uid := range pgs.assumedPods {
			pd.Assumed = append(pd.Assumed, string(uid))
		}
		for _, uid := range pgs.assignedPods.UnsortedList() {
			pd.Assigned = append(pd.Assigned, string(uid))
		}
		sortStrings(pd.All)
		sortStrings(pd.Unscheduled)
		sortStrings(pd.Assumed)
		sortStrings(pd.Assigned)
		d.PodGroups[key.String()] = pd
	}
	return d
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// TestSnapshot_UndoLogRestoreProperty runs randomized sequences of AddPod and
// RemovePod inside a mutation session and verifies that EndMutations restores
// the snapshot to a state deep-equal to the pre-session state — including
// slice order, per-node generations, PVC refcounts, and pod group state.
func TestSnapshot_UndoLogRestoreProperty(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
	})

	for _, seed := range []int64{1, 7, 42} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			rng := rand.New(rand.NewSource(seed))

			const numNodes = 12
			var nodes []*v1.Node
			var pods []*v1.Pod
			for i := 0; i < numNodes; i++ {
				nodes = append(nodes, st.MakeNode().Name(fmt.Sprintf("node-%d", i)).Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "64", v1.ResourceMemory: "256Gi", v1.ResourcePods: "200"}).Obj())
			}
			// Seed pods: mixture of plain, affinity, anti-affinity, PVC users.
			podID := 0
			makePod := func(nodeName string) *v1.Pod {
				podID++
				p := st.MakePod().Name(fmt.Sprintf("p-%d", podID)).Namespace("ns").UID(fmt.Sprintf("p-%d", podID)).Node(nodeName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m", v1.ResourceMemory: "64Mi"})
				switch rng.Intn(5) {
				case 0:
					p = p.PodAffinityExists("k", "zone", st.PodAffinityWithRequiredReq)
				case 1:
					p = p.PodAntiAffinityExists("k", "zone", st.PodAntiAffinityWithRequiredReq)
				case 2:
					p = p.PVC(fmt.Sprintf("pvc-%d", rng.Intn(5)))
				case 3:
					// Pod group member: exercises the podGroupMembership
					// capture/restore path of the undo log.
					p = p.PodGroupName(fmt.Sprintf("pg-%d", rng.Intn(3)))
				}
				return p.Obj()
			}
			for i := 0; i < 60; i++ {
				pods = append(pods, makePod(fmt.Sprintf("node-%d", rng.Intn(numNodes))))
			}
			s := NewSnapshot(pods, nodes)

			before := dumpSnapshot(s)

			if err := s.StartMutations(); err != nil {
				t.Fatalf("StartMutations failed: %v", err)
			}

			// Track current occupancy so removals target existing pods.
			type placed struct {
				pod      *v1.Pod
				nodeName string
			}
			var present []placed
			for _, p := range pods {
				present = append(present, placed{pod: p, nodeName: p.Spec.NodeName})
			}
			var removed []placed

			const ops = 400
			for i := 0; i < ops; i++ {
				switch op := rng.Intn(3); {
				case op == 0 || len(present) == 0: // add a brand new pod
					nodeName := fmt.Sprintf("node-%d", rng.Intn(numNodes+2)) // may hit unknown nodes
					pod := makePod(nodeName)
					pi, err := framework.NewPodInfo(pod)
					if err != nil {
						t.Fatalf("NewPodInfo: %v", err)
					}
					if err := s.AddPod(pi, nodeName); err != nil {
						t.Fatalf("op %d: AddPod: %v", i, err)
					}
					present = append(present, placed{pod: pod, nodeName: nodeName})
				case op == 1: // remove an existing pod
					j := rng.Intn(len(present))
					v := present[j]
					if err := s.RemovePod(logger, v.pod, v.nodeName); err != nil {
						t.Fatalf("op %d: RemovePod: %v", i, err)
					}
					present = append(present[:j], present[j+1:]...)
					removed = append(removed, v)
				default: // re-add a previously removed pod (reprieval)
					if len(removed) == 0 {
						continue
					}
					j := rng.Intn(len(removed))
					v := removed[j]
					pi, err := framework.NewPodInfo(v.pod)
					if err != nil {
						t.Fatalf("NewPodInfo: %v", err)
					}
					if err := s.AddPod(pi, v.nodeName); err != nil {
						t.Fatalf("op %d: re-AddPod: %v", i, err)
					}
					removed = append(removed[:j], removed[j+1:]...)
					present = append(present, placed{pod: v.pod, nodeName: v.nodeName})
				}
			}

			if err := s.EndMutations(); err != nil {
				t.Fatalf("EndMutations failed: %v", err)
			}

			after := dumpSnapshot(s)
			if diff := cmp.Diff(before, after); diff != "" {
				t.Errorf("snapshot not restored exactly (seed %d) (-before +after):\n%s", seed, diff)
			}

			// A second session on the restored snapshot must behave identically.
			if err := s.StartMutations(); err != nil {
				t.Fatalf("second StartMutations failed: %v", err)
			}
			pod := makePod("node-0")
			pi, _ := framework.NewPodInfo(pod)
			if err := s.AddPod(pi, "node-0"); err != nil {
				t.Fatalf("second session AddPod: %v", err)
			}
			if err := s.EndMutations(); err != nil {
				t.Fatalf("second EndMutations failed: %v", err)
			}
			final := dumpSnapshot(s)
			if diff := cmp.Diff(before, final); diff != "" {
				t.Errorf("snapshot not restored after second session (-before +after):\n%s", diff)
			}
		})
	}
}

// BenchmarkMutationSession measures a full StartMutations -> k mutations ->
// EndMutations session. With the undo log the cost depends on k, not on the
// cluster size; with the previous deep-copy backup it was O(nodes + pods) per
// session regardless of k.
func BenchmarkMutationSession(b *testing.B) {
	for _, numNodes := range []int{100, 1000, 5000} {
		for _, k := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("nodes=%d/mutations=%d", numNodes, k), func(b *testing.B) {
				logger, _ := ktesting.NewTestContext(b)
				var nodes []*v1.Node
				var pods []*v1.Pod
				podID := 0
				for i := 0; i < numNodes; i++ {
					nodeName := fmt.Sprintf("node-%d", i)
					nodes = append(nodes, st.MakeNode().Name(nodeName).Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "64", v1.ResourceMemory: "256Gi", v1.ResourcePods: "200"}).Obj())
					for j := 0; j < 30; j++ {
						podID++
						pods = append(pods, st.MakePod().Name(fmt.Sprintf("p-%d", podID)).Namespace("ns").UID(fmt.Sprintf("p-%d", podID)).Node(nodeName).Req(map[v1.ResourceName]string{v1.ResourceCPU: "100m"}).Obj())
					}
				}
				s := NewSnapshot(pods, nodes)
				victims := pods[:k]

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := s.StartMutations(); err != nil {
						b.Fatal(err)
					}
					for _, v := range victims {
						if err := s.RemovePod(logger, v, v.Spec.NodeName); err != nil {
							b.Fatal(err)
						}
					}
					if err := s.EndMutations(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// TestSnapshot_UndoLog_PodGroupMembershipRestore verifies that reverting a
// session mutation restores a pod's EXACT pre-mutation membership in its pod
// group state — in particular for pods that were already tracked by the group
// before the mutation. This is the main production shape: podgrouppreemption
// materializes assignments for preemptor pods that the group already tracks as
// unscheduled (AddPod moves them unscheduled -> assigned), and removes victim
// pods the group tracks as assigned. The restore must move memberships back,
// not blindly delete them.
func TestSnapshot_UndoLog_PodGroupMembershipRestore(t *testing.T) {
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.GenericWorkload: true,
	})
	logger, _ := ktesting.NewTestContext(t)

	node := st.MakeNode().Name("node-1").Capacity(map[v1.ResourceName]string{v1.ResourceCPU: "8", v1.ResourcePods: "50"}).Obj()
	// A preemptor pod: tracked by the group as unscheduled (no node yet).
	preemptor := st.MakePod().Name("preemptor").Namespace("ns").UID("preemptor").PodGroupName("pg-1").Obj()
	// A victim pod: tracked by the group as assigned (running on node-1).
	victim := st.MakePod().Name("victim").Namespace("ns").UID("victim").Node("node-1").PodGroupName("pg-1").Obj()

	s := NewSnapshot([]*v1.Pod{preemptor, victim}, []*v1.Node{node})

	pgKey := fwk.PodGroupKey("ns", "pg-1")
	pgs, ok := s.podGroupStates[pgKey]
	if !ok {
		t.Fatal("pod group state not found")
	}
	// Pre-session expectations.
	if !pgs.unscheduledPods.Has(preemptor.UID) || pgs.assignedPods.Has(preemptor.UID) {
		t.Fatal("preemptor should start unscheduled")
	}
	if !pgs.assignedPods.Has(victim.UID) {
		t.Fatal("victim should start assigned")
	}
	origPreemptorPtr := pgs.allPods[preemptor.UID]
	origVictimPtr := pgs.allPods[victim.UID]
	origGeneration := pgs.generation

	if err := s.StartMutations(); err != nil {
		t.Fatalf("StartMutations: %v", err)
	}

	// Remove the victim (its group membership: assigned -> gone)...
	if err := s.RemovePod(logger, victim, "node-1"); err != nil {
		t.Fatalf("RemovePod(victim): %v", err)
	}
	if pgs.assignedPods.Has(victim.UID) {
		t.Fatal("victim should not be assigned after RemovePod")
	}

	// ...materialize the preemptor's assignment: the pod is ALREADY tracked by
	// the group (unscheduled); AddPod moves it unscheduled -> assigned.
	assigned := preemptor.DeepCopy()
	assigned.Spec.NodeName = "node-1"
	pi, err := framework.NewPodInfo(assigned)
	if err != nil {
		t.Fatalf("NewPodInfo: %v", err)
	}
	if err := s.AddPod(pi, "node-1"); err != nil {
		t.Fatalf("AddPod(preemptor): %v", err)
	}
	if !pgs.assignedPods.Has(preemptor.UID) || pgs.unscheduledPods.Has(preemptor.UID) {
		t.Fatal("preemptor should be assigned during the session")
	}

	// Reprieve the victim (already absent from the group; AddPod re-adds it).
	victimInfo, err := framework.NewPodInfo(victim)
	if err != nil {
		t.Fatalf("NewPodInfo(victim): %v", err)
	}
	if err := s.AddPod(victimInfo, "node-1"); err != nil {
		t.Fatalf("AddPod(victim reprieval): %v", err)
	}
	if !pgs.assignedPods.Has(victim.UID) {
		t.Fatal("victim should be assigned again after reprieval")
	}

	if err := s.EndMutations(); err != nil {
		t.Fatalf("EndMutations: %v", err)
	}

	// The preemptor must be back to unscheduled — NOT deleted from the group.
	if !pgs.unscheduledPods.Has(preemptor.UID) {
		t.Error("preemptor must be unscheduled again after restore")
	}
	if pgs.assignedPods.Has(preemptor.UID) {
		t.Error("preemptor must not remain assigned after restore")
	}
	// The victim must be back to assigned.
	if !pgs.assignedPods.Has(victim.UID) {
		t.Error("victim must be assigned again after restore")
	}
	// Even the stored pod pointers and the group generation are restored.
	if pgs.allPods[preemptor.UID] != origPreemptorPtr {
		t.Error("preemptor allPods entry must be the original pointer")
	}
	if pgs.allPods[victim.UID] != origVictimPtr {
		t.Error("victim allPods entry must be the original pointer")
	}
	if pgs.generation != origGeneration {
		t.Errorf("pod group generation not restored: want %d, got %d", origGeneration, pgs.generation)
	}
}
