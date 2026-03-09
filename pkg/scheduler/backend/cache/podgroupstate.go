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
	"maps"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

var generation atomic.Int64

// nextPodGroupGeneration increments generation numbers monotonically for a pod group state (instead of per-instance increment)
// to prevent generation reset or collision when a pod group is deleted and recreated with the same name.
func nextPodGroupGeneration() int64 {
	return generation.Add(1)
}

// DefaultPodGroupSchedulingTimeoutDuration defines how long the gang pods should
// wait at the permit stage for a quorum before being rejected.
var DefaultPodGroupSchedulingTimeoutDuration = 5 * time.Minute

// podGroupKey uniquely identifies a specific instance of a PodGroup.
type podGroupKey struct {
	name      string
	namespace string
}

func (pgk podGroupKey) GetName() string {
	return pgk.name
}

func (pgk podGroupKey) GetNamespace() string {
	return pgk.namespace
}

func (pgk podGroupKey) String() string {
	return pgk.namespace + "/" + pgk.GetName()
}

var _ klog.KMetadata = &podGroupKey{}

func newPodGroupKey(namespace string, name string) podGroupKey {
	return podGroupKey{
		namespace: namespace,
		name:      name,
	}
}

// podGroupStateData holds data and functionality shared between podGroupState and podGroupStateSnapshot.
type podGroupStateData struct {
	// generation gets bumped whenever the data is changed.
	// It's used to detect changes and avoid unnecessary cloning when taking a snapshot.
	generation int64
	// allPods tracks all pods belonging to the group that are known to the scheduler.
	allPods map[types.UID]*v1.Pod
	// unscheduledPods tracks all pods that are unscheduled for this group,
	// i.e., are neither assumed nor assigned.
	unscheduledPods sets.Set[types.UID]
	// assumedPods tracks pods that have reached the Reserve stage and are waiting
	// for the rest of the gang to arrive before being allowed to bind.
	assumedPods sets.Set[types.UID]
	// assignedPods tracks all pods belonging to the group that are assigned (bound).
	assignedPods sets.Set[types.UID]
	// schedulingDeadline stores the time at which the gang will time out.
	// It is initialized when the first pod from the group enters the Permit stage.
	schedulingDeadline *time.Time
}

func newPodGroupStateData() podGroupStateData {
	return podGroupStateData{
		allPods:         make(map[types.UID]*v1.Pod),
		unscheduledPods: sets.New[types.UID](),
		assumedPods:     sets.New[types.UID](),
		assignedPods:    sets.New[types.UID](),
	}
}

// addPod adds the pod to this group.
// Depending on the NodeName, it can insert the pod into either assignedPods or unscheduledPods.
func (d *podGroupStateData) addPod(pod *v1.Pod) {
	d.generation = nextPodGroupGeneration()
	d.allPods[pod.UID] = pod
	if pod.Spec.NodeName != "" {
		d.assignedPods.Insert(pod.UID)
	} else {
		d.unscheduledPods.Insert(pod.UID)
	}
}

// updatePod updates the pod in this group.
// In case of binding, it moves the pod to assignedPods.
func (d *podGroupStateData) updatePod(oldPod, newPod *v1.Pod) {
	d.generation = nextPodGroupGeneration()
	d.allPods[newPod.UID] = newPod
	if oldPod.Spec.NodeName == "" && newPod.Spec.NodeName != "" {
		d.assignedPods.Insert(newPod.UID)
		// Clear pod from unscheduled and assumed when it is assigned.
		d.unscheduledPods.Delete(newPod.UID)
		d.assumedPods.Delete(newPod.UID)
	}
}

// deletePod removes the pod from this pod group state.
func (d *podGroupStateData) deletePod(podUID types.UID) {
	d.generation = nextPodGroupGeneration()
	delete(d.allPods, podUID)
	d.unscheduledPods.Delete(podUID)
	d.assumedPods.Delete(podUID)
	d.assignedPods.Delete(podUID)
}

// assumePod marks a pod as assumed within the pod group state.
func (d *podGroupStateData) assumePod(pod *v1.Pod) {
	d.generation = nextPodGroupGeneration()
	d.allPods[pod.UID] = pod
	d.assumedPods.Insert(pod.UID)
	d.unscheduledPods.Delete(pod.UID)
}

// forgetPod moves a pod back from the assumed state to unscheduled within the pod group state.
func (d *podGroupStateData) forgetPod(podUID types.UID) {
	d.generation = nextPodGroupGeneration()
	d.unscheduledPods.Insert(podUID)
	d.assumedPods.Delete(podUID)
}

// empty returns true when the pod group state contains no pods.
func (d *podGroupStateData) empty() bool {
	return len(d.allPods) == 0
}

// deepCopy returns a deep copy of the pod group state data.
func (d *podGroupStateData) deepCopy() podGroupStateData {
	var deadline *time.Time
	if d.schedulingDeadline != nil {
		t := *d.schedulingDeadline
		deadline = &t
	}
	return podGroupStateData{
		generation:         d.generation,
		allPods:            maps.Clone(d.allPods),
		unscheduledPods:    d.unscheduledPods.Clone(),
		assumedPods:        d.assumedPods.Clone(),
		assignedPods:       d.assignedPods.Clone(),
		schedulingDeadline: deadline,
	}
}

// unscheduledPodsMap returns all unscheduled pods for this pod group.
func (d *podGroupStateData) unscheduledPodsMap() map[string]*v1.Pod {
	result := make(map[string]*v1.Pod, len(d.unscheduledPods))
	for podUID := range d.unscheduledPods {
		pod := d.allPods[podUID]
		result[pod.Name] = pod
	}
	return result
}

// schedulingTimeout returns the remaining time until the scheduling deadline,
// creating or refreshing the deadline if it doesn't exist or has passed.
func (d *podGroupStateData) schedulingTimeout() time.Duration {
	now := time.Now()
	// A new deadline is set if one doesn't exist, or if the old one has passed.
	// This allows a new attempt to form a gang after a previous attempt timed out.
	if d.schedulingDeadline == nil || d.schedulingDeadline.Before(now) {
		d.generation = nextPodGroupGeneration()
		t := now.Add(DefaultPodGroupSchedulingTimeoutDuration)
		d.schedulingDeadline = &t
	}
	return d.schedulingDeadline.Sub(now)
}

// podGroupState holds the runtime state of a pod group.
type podGroupState struct {
	lock sync.RWMutex
	podGroupStateData
}

func newPodGroupState() *podGroupState {
	return &podGroupState{podGroupStateData: newPodGroupStateData()}
}

// snapshot returns a deep copy of the live pod group state as an immutable snapshot.
// It must be called under the cache lock.
func (pgs *podGroupState) snapshot() *podGroupStateSnapshot {
	return &podGroupStateSnapshot{podGroupStateData: pgs.podGroupStateData.deepCopy()}
}

// empty returns true when the group contains no pods.
// It must be called under the cache lock.
func (pgs *podGroupState) empty() bool {
	return pgs.podGroupStateData.empty()
}

// forgetPod moves a pod back from the assumed state to unscheduled.
// It must be called under the cache lock.
func (pgs *podGroupState) forgetPod(podUID types.UID) {
	pgs.podGroupStateData.forgetPod(podUID)
}

// AllPods returns the UIDs of all pods known to the scheduler for this group.
func (pgs *podGroupState) AllPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return sets.KeySet(pgs.podGroupStateData.allPods)
}

// UnscheduledPods returns all pods that are unscheduled for this group,
// i.e., are neither assumed nor assigned.
// The returned map type corresponds to the argument of the PodActivator.Activate method.
func (pgs *podGroupState) UnscheduledPods() map[string]*v1.Pod {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.unscheduledPodsMap()
}

// AssumedPods returns the UIDs of all pods for this group in the assumed state,
// i.e., that have passed the Reserve stage.
func (pgs *podGroupState) AssumedPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.assumedPods.Clone()
}

// AssignedPods returns the UIDs of all pods already assigned (bound) for this group.
func (pgs *podGroupState) AssignedPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.podGroupStateData.assignedPods.Clone()
}

// SchedulingTimeout returns the remaining time until the pod group scheduling times out.
// A new deadline is created if one doesn't exist, or if the previous one has expired.
func (pgs *podGroupState) SchedulingTimeout() time.Duration {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	return pgs.podGroupStateData.schedulingTimeout()
}

// podGroupStateSnapshot is an immutable, point-in-time copy of a podGroupState.
// It is taken before a pod group scheduling cycle and used to track states of pods
// during the cycle without modifying the live state of pods.
type podGroupStateSnapshot struct {
	podGroupStateData
}

// assumePod marks a pod within the pod group state snapshot as assumed.
func (s *podGroupStateSnapshot) assumePod(pod *v1.Pod) {
	s.podGroupStateData.assumePod(pod)
}

// forgetPod removes a pod from the assumed state within the snapshot.
func (s *podGroupStateSnapshot) forgetPod(podUID types.UID) {
	s.podGroupStateData.forgetPod(podUID)
}

// AllPods returns the UIDs of all pods known to the scheduler for this group.
func (s *podGroupStateSnapshot) AllPods() sets.Set[types.UID] {
	return sets.KeySet(s.podGroupStateData.allPods)
}

// UnscheduledPods returns all pods that are unscheduled for this group.
func (s *podGroupStateSnapshot) UnscheduledPods() map[string]*v1.Pod {
	return s.podGroupStateData.unscheduledPodsMap()
}

// AssumedPods returns the UIDs of all assumed pods for this group.
func (s *podGroupStateSnapshot) AssumedPods() sets.Set[types.UID] {
	return s.podGroupStateData.assumedPods
}

// AssignedPods returns the UIDs of all assigned (bound) pods for this group.
func (s *podGroupStateSnapshot) AssignedPods() sets.Set[types.UID] {
	return s.podGroupStateData.assignedPods
}

// SchedulingTimeout returns the remaining time until the pod group scheduling times out.
// A new deadline is created if one doesn't exist, or if the previous one has expired.
func (s *podGroupStateSnapshot) SchedulingTimeout() time.Duration {
	return s.podGroupStateData.schedulingTimeout()
}
