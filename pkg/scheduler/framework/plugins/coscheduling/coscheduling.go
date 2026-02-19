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

package coscheduling

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1alpha1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	pluginHelper "k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = names.Coscheduling
)

// Coscheduling is a plugin that facilitates best-effort scheduling for pods
// belonging to a Workload with a Basic scheduling policy.
type Coscheduling struct {
	handle                  fwk.Handle
	workloadLister          schedulinglisters.WorkloadLister
	podGroupPolicyEvaluator *pluginHelper.PodGroupPolicyEvaluator
}

var _ fwk.EnqueueExtensions = &Coscheduling{}
var _ fwk.PreEnqueuePlugin = &Coscheduling{}

func New(_ context.Context, _ runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &Coscheduling{
		handle:                  fh,
		workloadLister:          fh.SharedInformerFactory().Scheduling().V1alpha1().Workloads().Lister(),
		podGroupPolicyEvaluator: pluginHelper.New(fts.EnablePodGroupDesiredCount),
	}, nil
}

func (pl *Coscheduling) Name() string {
	return Name
}

func (pl *Coscheduling) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// A new pod being added might be the one that help meeting its DesiredCount requirement.
		// Workload reference is immutable, so there is no need to subscribe on Pod/Update event.
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Add}, QueueingHintFn: pl.podGroupPolicyEvaluator.IsReEvaluatingNeedAfterPodAdded},
		// A Workload being added can be making a waiting gang schedulable.
		// Workload's PodGroups are immutable, so there's no need to handle Workload/Update event.
		{Event: fwk.ClusterEvent{Resource: fwk.Workload, ActionType: fwk.Add}, QueueingHintFn: pl.podGroupPolicyEvaluator.IsReEvaluatingNeedAfterWorkloadAdded},
	}, nil
}

func (pl *Coscheduling) PreEnqueue(ctx context.Context, pod *v1.Pod) *fwk.Status {
	if pod.Spec.WorkloadRef == nil {
		return nil
	}

	namespace := pod.Namespace
	workloadRef := pod.Spec.WorkloadRef

	workload, err := pl.workloadLister.Workloads(namespace).Get(workloadRef.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The pod is unschedulable until its Workload object is created.
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("waiting for pods's workload %q to appear in scheduling queue", workloadRef.Name))
		}
		klog.FromContext(ctx).Error(err, "Failed to get workload", "pod", klog.KObj(pod), "workloadRef", pod.Spec.WorkloadRef)
		return fwk.AsStatus(fmt.Errorf("failed to get workload %s/%s", namespace, workloadRef.Name))
	}

	policy, ok := pluginHelper.PodGroupPolicy(workload, workloadRef.PodGroup)
	if !ok {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("pod group %q doesn't exist for a workload %q", workloadRef.PodGroup, workload.Name))
	}
	// This plugin only cares about pods with a Basic scheduling policy.
	if policy.Basic == nil {
		return nil
	}

	podGroupInfo, err := pl.handle.WorkloadManager().PodGroupState(namespace, workloadRef)
	if err != nil {
		return fwk.AsStatus(err)
	}
	allPods := podGroupInfo.AllPods()

	return pl.podGroupPolicyEvaluator.PreEnqueue(pod, policy, len(allPods))
}
