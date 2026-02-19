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
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	schedulingapi "k8s.io/api/scheduling/v1alpha1"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func Test_IsReEvaluatingNeedAfterWorkloadAdded(t *testing.T) {
	tests := []struct {
		name         string
		pod          *v1.Pod
		newWorkload  *schedulingapi.Workload
		expectedHint fwk.QueueingHint
	}{
		{
			name:         "add a workload which matches the pod's workload name",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			newWorkload:  st.MakeWorkload().Name("w1").PodGroup(st.MakePodGroup().Name("pg").MinCount(1).Obj()).Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "add a workload which doesn't match the pod's workload name",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w2", PodGroup: "pg"}).Obj(),
			newWorkload:  st.MakeWorkload().Name("w1").PodGroup(st.MakePodGroup().Name("pg").MinCount(1).Obj()).Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "add a workload which doesn't match the pod's workload namespace",
			pod:          st.MakePod().Namespace("ns1").Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			newWorkload:  st.MakeWorkload().Namespace("ns2").Name("w1").PodGroup(st.MakePodGroup().Name("pg").MinCount(1).Obj()).Obj(),
			expectedHint: fwk.QueueSkip,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)

			e := New(true)

			actualHint, err := e.IsReEvaluatingNeedAfterWorkloadAdded(logger, tc.pod, nil, tc.newWorkload)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.expectedHint, actualHint); diff != "" {
				t.Errorf("Expected QueuingHint doesn't match (-want,+got):\n%s", diff)
			}
		})
	}
}

func Test_IsReEvaluatingNeedAfterPodAdded(t *testing.T) {
	tests := []struct {
		name         string
		pod          *v1.Pod
		newPod       *v1.Pod
		expectedHint fwk.QueueingHint
	}{
		{
			name:         "add a newPod which matches the pod's workload and pod group",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			newPod:       st.MakePod().WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "add a newPod which matches the pod's workload, pod group and replica key",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg", PodGroupReplicaKey: "3"}).Obj(),
			newPod:       st.MakePod().WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg", PodGroupReplicaKey: "3"}).Obj(),
			expectedHint: fwk.Queue,
		},
		{
			name:         "add a newPod which doesn't match the pod's namespace",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			newPod:       st.MakePod().Namespace("foo").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "add a newPod which doesn't match the pod's workload name",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			newPod:       st.MakePod().WorkloadRef(&v1.WorkloadReference{Name: "w2", PodGroup: "pg"}).Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "add a newPod which doesn't match the pod's pod group name",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg"}).Obj(),
			newPod:       st.MakePod().WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg2"}).Obj(),
			expectedHint: fwk.QueueSkip,
		},
		{
			name:         "add a newPod which doesn't match the pod's replica key",
			pod:          st.MakePod().Name("p").WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg", PodGroupReplicaKey: "3"}).Obj(),
			newPod:       st.MakePod().WorkloadRef(&v1.WorkloadReference{Name: "w1", PodGroup: "pg", PodGroupReplicaKey: "4"}).Obj(),
			expectedHint: fwk.QueueSkip,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)

			e := New(true)

			actualHint, err := e.IsReEvaluatingNeedAfterPodAdded(logger, tc.pod, nil, tc.newPod)
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.expectedHint, actualHint); diff != "" {
				t.Errorf("Expected QueuingHint doesn't match (-want,+got):\n%s", diff)
			}
		})
	}
}
