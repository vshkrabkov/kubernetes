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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	apidispatcher "k8s.io/kubernetes/pkg/scheduler/backend/api_dispatcher"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	apicalls "k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// Tests wait for requeue completion before inspecting queue state.
type failureQueueObserver struct {
	*internalqueue.PriorityQueue
	requeued chan struct{}
}

func (q *failureQueueObserver) AddUnschedulablePodIfNotPresent(logger klog.Logger, info *framework.QueuedPodInfo, cycle int64) error {
	err := q.PriorityQueue.AddUnschedulablePodIfNotPresent(logger, info, cycle)
	close(q.requeued)
	return err
}

type failureHandlerTest struct {
	ctx        context.Context
	logger     klog.Logger
	pod        *v1.Pod
	info       *framework.QueuedPodInfo
	client     *fake.Clientset
	store      cache.Store
	queue      *failureQueueObserver
	dispatcher *apidispatcher.APIDispatcher
	scheduler  *Scheduler
	framework  framework.Framework
}

func newFailureHandlerTest(t *testing.T, async bool) *failureHandlerTest {
	t.Helper()
	logger, ctx := ktesting.NewTestContext(t)
	pod := st.MakePod().Name("test-pod").Namespace("default").UID("uid-1").Obj()
	client := fake.NewClientset(pod)
	factory := informers.NewSharedInformerFactory(client, 0)
	store := factory.Core().V1().Pods().Informer().GetStore()
	if err := store.Add(pod); err != nil {
		t.Fatal(err)
	}
	var dispatcher *apidispatcher.APIDispatcher
	if async {
		dispatcher = apidispatcher.New(client, 16, apicalls.Relevances)
		dispatcher.Run(logger)
		t.Cleanup(dispatcher.Close)
	}
	recorder := metrics.NewMetricsAsyncRecorder(10, time.Millisecond, ctx.Done())
	queue := &failureQueueObserver{
		PriorityQueue: internalqueue.NewPriorityQueue(nil, factory, internalqueue.WithMetricsRecorder(recorder), internalqueue.WithAPIDispatcher(dispatcher)),
		requeued:      make(chan struct{}),
	}
	schedCache := internalcache.New(ctx, dispatcher, false, false)
	scheduler, f, err := initScheduler(ctx, schedCache, queue, dispatcher, client, factory)
	if err != nil {
		t.Fatal(err)
	}
	queue.Add(ctx, pod)
	entity, err := queue.Pop(logger)
	if err != nil {
		t.Fatal(err)
	}
	return &failureHandlerTest{ctx, logger, pod, entity.(*framework.QueuedPodInfo), client, store, queue, dispatcher, scheduler, f}
}

func (e *failureHandlerTest) fail(ctx context.Context) {
	e.scheduler.FailureHandler(ctx, e.framework, e.info, fwk.NewStatus(fwk.Unschedulable, "test scheduling failure"), nil, time.Now())
}

func waitForFailureSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for failure-handler operation")
	}
}

func assertFailureQueue(t *testing.T, e *failureHandlerTest, want *v1.Pod) {
	t.Helper()
	if diff := cmp.Diff(want, getPodFromPriorityQueue(e.queue.PriorityQueue, e.pod)); diff != "" {
		t.Errorf("unexpected queued pod (-want,+got):\n%s", diff)
	}
	if len(e.queue.InFlightPods()) != 0 {
		t.Errorf("failure left pods in flight: %v", e.queue.InFlightPods())
	}
}

func blockPodStatusPatches(t *testing.T, client *fake.Clientset, patchErr error) (<-chan struct{}, func()) {
	t.Helper()
	started, unblock := make(chan struct{}), make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		startedOnce.Do(func() { close(started) })
		<-unblock
		// Keep the informer snapshot independent of status writes in the fake client.
		return true, &v1.Pod{}, patchErr
	})
	return started, release
}

func TestFailureHandler_AsyncPatchDelaysRequeue(t *testing.T) {
	for _, patchErr := range []error{nil, errors.New("patch failed")} {
		t.Run(fmt.Sprintf("patch error: %v", patchErr), func(t *testing.T) {
			e := newFailureHandlerTest(t, true)
			started, release := blockPodStatusPatches(t, e.client, patchErr)
			e.fail(e.ctx)
			waitForFailureSignal(t, started)
			if getPodFromPriorityQueue(e.queue.PriorityQueue, e.pod) != nil || len(e.queue.InFlightPods()) != 1 {
				t.Fatal("pod must stay in flight and out of the queue while PATCH is blocked")
			}
			release()
			waitForFailureSignal(t, e.queue.requeued)
			assertFailureQueue(t, e, e.pod)
		})
	}
}

func TestFailureHandler_SyncPatchRunsAfterRequeue(t *testing.T) {
	e := newFailureHandlerTest(t, false)
	patchCalled := false
	e.client.PrependReactor("patch", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		patchCalled = true
		assertFailureQueue(t, e, e.pod)
		return true, &v1.Pod{}, errors.New("patch failed")
	})
	e.fail(e.ctx)
	if !patchCalled {
		t.Fatal("status PATCH was not executed")
	}
	assertFailureQueue(t, e, e.pod)
}

func TestFailureHandler_CancelBindingCycleDuringPatch(t *testing.T) {
	e := newFailureHandlerTest(t, true)
	started, release := blockPodStatusPatches(t, e.client, nil)
	cycleCtx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	e.fail(cycleCtx)
	waitForFailureSignal(t, started)
	cancel()
	select {
	case <-e.queue.requeued:
		t.Fatal("cycle cancellation requeued the pod before PATCH completion")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	waitForFailureSignal(t, e.queue.requeued)
	assertFailureQueue(t, e, e.pod)
}

func TestFailureHandler_DispatcherClose(t *testing.T) {
	e := newFailureHandlerTest(t, true)
	started, _ := blockPodStatusPatches(t, e.client, nil)
	e.fail(e.ctx)
	waitForFailureSignal(t, started)
	e.dispatcher.Close()
	// The fake HTTP request stays blocked, so only shutdown can release this waiter.
	waitForFailureSignal(t, e.queue.requeued)
	assertFailureQueue(t, e, e.pod)
}

func TestFailureHandler_PodChangesBeforeFailure(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, change := range []string{"unchanged", "updated", "deleted", "recreated", "bound", "deferred resize"} {
			t.Run(fmt.Sprintf("%s/async=%v", change, async), func(t *testing.T) {
				e := newFailureHandlerTest(t, async)
				latest := e.pod.DeepCopy()
				want := latest
				switch change {
				case "updated":
					latest.Labels = map[string]string{"version": "2"}
				case "deleted":
					if err := e.store.Delete(e.pod); err != nil {
						t.Fatal(err)
					}
					e.queue.Delete(e.logger, e.pod)
					want = nil
				case "recreated":
					latest.UID = "uid-2"
					want = nil
				case "bound":
					latest.Spec.NodeName = "node1"
					want = nil
				case "deferred resize":
					featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.InPlacePodVerticalScalingSchedulerPreemption, true)
					latest.Spec.NodeName = "node1"
					latest.Status.Conditions = []v1.PodCondition{{Type: v1.PodResizePending, Status: v1.ConditionTrue, Reason: v1.PodReasonDeferred}}
					e.info.PodInfo = mustNewPodInfo(t, latest)
					e.scheduler.inPlacePodVerticalScalingSchedulerPreemptionEnabled = true
				}
				if change != "deleted" {
					if err := e.store.Update(latest); err != nil {
						t.Fatal(err)
					}
					if change != "recreated" {
						e.queue.Update(e.ctx, e.pod, latest)
					}
				}
				// Skip PATCH execution details here; separate tests control its ordering.
				_, release := blockPodStatusPatches(t, e.client, nil)
				release()
				e.fail(e.ctx)
				if want != nil {
					waitForFailureSignal(t, e.queue.requeued)
				}
				assertFailureQueue(t, e, want)
			})
		}
	}
}

func TestFailureHandler_PodGroupWaitsForPatch(t *testing.T) {
	e := newFailureHandlerTest(t, true)
	pod := e.pod.DeepCopy()
	groupName := "test-group"
	pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{PodGroupName: &groupName}
	if err := e.store.Update(pod); err != nil {
		t.Fatal(err)
	}
	e.queue.Update(e.ctx, e.pod, pod)
	e.scheduler.genericWorkloadEnabled = true
	started, release := blockPodStatusPatches(t, e.client, nil)
	returned := make(chan struct{})
	go func() {
		e.fail(e.ctx)
		close(returned)
	}()
	waitForFailureSignal(t, started)
	waitForFailureSignal(t, e.queue.requeued)
	assertFailureQueue(t, e, pod)
	select {
	case <-returned:
		t.Fatal("pod group failure handler returned before PATCH completion")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	waitForFailureSignal(t, returned)
}

func TestFailureHandler_DispatchErrorRequeuesPod(t *testing.T) {
	for _, podGroupMember := range []bool{false, true} {
		t.Run(fmt.Sprintf("pod group member: %v", podGroupMember), func(t *testing.T) {
			e := newFailureHandlerTest(t, true)
			pod := e.pod.DeepCopy()
			if podGroupMember {
				groupName := "test-group"
				pod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{PodGroupName: &groupName}
				if err := e.store.Update(pod); err != nil {
					t.Fatal(err)
				}
				e.queue.Update(e.ctx, e.pod, pod)
				e.scheduler.genericWorkloadEnabled = true
			}
			// Reject submission before any worker can execute the PATCH.
			e.dispatcher.Close()
			returned := make(chan struct{})
			go func() {
				e.fail(e.ctx)
				close(returned)
			}()
			waitForFailureSignal(t, returned)
			waitForFailureSignal(t, e.queue.requeued)
			assertFailureQueue(t, e, pod)
			if actions := e.client.Actions(); len(actions) != 0 {
				t.Fatalf("expected no API calls after dispatcher close, got %v", actions)
			}
		})
	}
}

func TestFailureHandler_DiscardedPodDoesNotWaitForPatch(t *testing.T) {
	for _, change := range []string{"deleted", "bound"} {
		t.Run(change, func(t *testing.T) {
			e := newFailureHandlerTest(t, true)
			if change == "deleted" {
				if err := e.store.Delete(e.pod); err != nil {
					t.Fatal(err)
				}
			} else {
				pod := e.pod.DeepCopy()
				pod.Spec.NodeName = "node1"
				if err := e.store.Update(pod); err != nil {
					t.Fatal(err)
				}
			}
			started, _ := blockPodStatusPatches(t, e.client, nil)
			returned := make(chan struct{})
			go func() {
				e.fail(e.ctx)
				close(returned)
			}()
			waitForFailureSignal(t, started)
			waitForFailureSignal(t, returned)
			assertFailureQueue(t, e, nil)
		})
	}
}

func TestFailureHandler_StatusUsesLatestGeneration(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			e := newFailureHandlerTest(t, async)
			latest := e.pod.DeepCopy()
			latest.Generation = 2
			latest.Status.Conditions = []v1.PodCondition{{
				Type: v1.PodScheduled, Status: v1.ConditionTrue, ObservedGeneration: 1,
			}}
			if err := e.store.Update(latest); err != nil {
				t.Fatal(err)
			}
			e.queue.Update(e.ctx, e.pod, latest)

			e.fail(e.ctx)
			waitForFailureSignal(t, e.queue.requeued)
			updated, err := e.client.CoreV1().Pods(e.pod.Namespace).Get(e.ctx, e.pod.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(updated.Status.Conditions) != 1 {
				t.Fatalf("expected one scheduling condition, got %v", updated.Status.Conditions)
			}
			if got := updated.Status.Conditions[0].ObservedGeneration; got != latest.Generation {
				t.Errorf("status observed generation = %d, want informer generation %d", got, latest.Generation)
			}
		})
	}
}

func TestFailureHandler_NoErrorOnlyUpdatesNomination(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			e := newFailureHandlerTest(t, async)
			nomination := &fwk.NominatingInfo{NominatedNodeName: "node1", NominatingMode: fwk.ModeOverride}
			e.scheduler.FailureHandler(e.ctx, e.framework, e.info, nil, nomination, time.Now())
			waitForFailureSignal(t, e.queue.requeued)
			assertFailureQueue(t, e, e.pod)
			nominated := e.queue.NominatedPodsForNode("node1")
			if len(nominated) != 1 || nominated[0].GetPod().UID != e.pod.UID {
				t.Fatalf("expected attempted pod to be nominated for node1, got %v", nominated)
			}
			if actions := e.client.Actions(); len(actions) != 0 {
				t.Fatalf("expected no API calls for a status without an error, got %v", actions)
			}
		})
	}
}
