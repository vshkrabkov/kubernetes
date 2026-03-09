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

package cache

import (
	"testing"
	"time"

	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/utils/ptr"
)

func TestPodGroupState_AssumeForget(t *testing.T) {
	pgs := newPodGroupState()
	pod := st.MakePod().Namespace("ns1").Name("p1").UID("p1").PodGroupName("pg1").Obj()

	pgs.addPod(pod)
	if pgs.AssumedPods().Has(pod.UID) {
		t.Fatal("AssumedPods should be initially empty")
	}
	if !pgs.unscheduledPods.Has(pod.UID) {
		t.Fatal("Pod should be initially in UnscheduledPods")
	}

	pgs.assumePod(pod)
	if !pgs.AssumedPods().Has(pod.UID) {
		t.Fatal("Pod should be in AssumedPods after AssumePod")
	}
	if pgs.unscheduledPods.Has(pod.UID) {
		t.Fatal("UnscheduledPods should be empty after AssumePod")
	}

	pgs.forgetPod(pod.UID)
	if pgs.AssumedPods().Has(pod.UID) {
		t.Fatal("Pod should not be in AssumedPods after ForgetPod")
	}
	if !pgs.unscheduledPods.Has(pod.UID) {
		t.Fatal("Pod should be in UnscheduledPods after ForgetPod")
	}
}

func TestPodGroupState_SchedulingTimeout(t *testing.T) {
	pgs := newPodGroupState()

	timeout := pgs.SchedulingTimeout()
	if pgs.schedulingDeadline == nil {
		t.Fatal("Scheduling deadline should be set after SchedulingTimeout call, but is nil")
	}
	if timeout <= 0 {
		t.Errorf("Expected positive timeout duration, got %v", timeout)
	}

	// Sleep for a while to ensure that the time has increased,
	// especially when testing on Windows machines with lower resolution.
	time.Sleep(10 * time.Millisecond)

	deadline := *pgs.schedulingDeadline
	newTimeout := pgs.SchedulingTimeout()
	if !deadline.Equal(*pgs.schedulingDeadline) {
		t.Errorf("Previous deadline should not be changed: previous: %v, current: %v", deadline, *pgs.schedulingDeadline)
	}
	if newTimeout >= timeout {
		t.Errorf("Expected lower timeout duration: previous: %v, current: %v", timeout, newTimeout)
	}

	// Sleep for a while to ensure that the time has increased,
	// especially when testing on Windows machines with lower resolution.
	time.Sleep(10 * time.Millisecond)

	pgs.schedulingDeadline = ptr.To(time.Now().Add(-1 * time.Second))
	newTimeout = pgs.SchedulingTimeout()
	if deadline.Equal(*pgs.schedulingDeadline) {
		t.Error("Deadline should be reset after it has expired, but it wasn't")
	}
	if newTimeout <= 0 {
		t.Errorf("Expected positive timeout duration after reset, got %v", timeout)
	}
}

func TestPodGroupState_GenerationTracking(t *testing.T) {
	pgs := newPodGroupState()
	pod := st.MakePod().Namespace("ns1").Name("p1").UID("p1").PodGroupName("pg").Obj()
	assignedPod := st.MakePod().Namespace("ns1").Name("p1").UID("p1").Node("node1").
		PodGroupName("pg").Obj()

	tests := []struct {
		name   string
		action func()
	}{
		{"addPod", func() { pgs.addPod(pod) }},
		{"updatePod", func() { pgs.updatePod(pod, assignedPod) }},
		{"assumePod", func() { pgs.assumePod(pod) }},
		{"forgetPod", func() { pgs.forgetPod(pod.UID) }},
		{"deletePod", func() { pgs.deletePod(pod.UID) }},
		{"addPod, re-creates PodGroupState and increases its global generation", func() { pgs.addPod(pod) }},
	}
	prev := pgs.generation
	for _, tc := range tests {
		tc.action()
		if pgs.generation <= prev {
			t.Errorf("after %s: expected generation %d, got %d", tc.name, prev, pgs.generation)
		}
		prev = pgs.generation
	}
}

func TestPodGroupState_Clone(t *testing.T) {
	pgs := newPodGroupState()

	pod1 := st.MakePod().Namespace("ns1").Name("p1").UID("p1").
		PodGroupName("pg").Obj()
	pod2 := st.MakePod().Namespace("ns1").Name("p2").UID("p2").
		PodGroupName("pg").Obj()

	pgs.addPod(pod1)
	pgs.addPod(pod2)
	pgs.assumePod(pod2)

	snap := pgs.snapshot()

	// Clone has the same generation.
	if snap.generation != pgs.generation {
		t.Errorf("expected clone generation %d, got %d", pgs.generation, snap.generation)
	}

	// Clone contains both pods.
	if !snap.AllPods().Has(pod1.UID) || !snap.AllPods().Has(pod2.UID) {
		t.Error("expected both pods in clone's AllPods")
	}

	// Clone preserves pod1 as unscheduled.
	if _, ok := snap.UnscheduledPods()[pod1.Name]; !ok {
		t.Error("expected pod1 in clone's UnscheduledPods")
	}

	// Clone preserves pod2 as assumed.
	if !snap.AssumedPods().Has(pod2.UID) {
		t.Error("expected pod2 in clone's AssumedPods")
	}

	// Mutating the clone does not affect the original.
	snap.assumePod(pod1)
	if pgs.assumedPods.Has(pod1.UID) {
		t.Error("mutation to clone should not affect original's assumedPods")
	}

	// Mutating the original does not affect the clone.
	pod3 := st.MakePod().Namespace("ns1").Name("p3").UID("p3").
		PodGroupName("pg").Obj()
	pgs.addPod(pod3)
	if snap.AllPods().Has(pod3.UID) {
		t.Error("mutation to original should not affect clone's AllPods")
	}
}
