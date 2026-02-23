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
	v1 "k8s.io/api/core/v1"
	schedulingapi "k8s.io/api/scheduling/v1alpha1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// matchingWorkloadReference returns true if two pods belong to the same workload, including their pod group and replica key.
func matchingWorkloadReference(pod1, pod2 *v1.Pod) bool {
	return pod1.Spec.WorkloadRef != nil && pod2.Spec.WorkloadRef != nil && pod1.Namespace == pod2.Namespace && *pod1.Spec.WorkloadRef == *pod2.Spec.WorkloadRef
}

// IsSchedulableAfterPodAdded is a queueing hint function to evaluate if a newly added pod makes the target pod schedulable.
func IsSchedulableAfterPodAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
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

// IsSchedulableAfterWorkloadAdded is a queueing hint function to evaluate if a newly added workload makes the target pod schedulable.
func IsSchedulableAfterWorkloadAdded(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
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

// podGroupPolicy is a helper to find the policy for a specific pod group name in a workload.
func PodGroupPolicy(workload *schedulingapi.Workload, podGroupName string) (schedulingapi.PodGroupPolicy, bool) {
	for _, podGroup := range workload.Spec.PodGroups {
		if podGroup.Name == podGroupName {
			return podGroup.Policy, true
		}
	}
	return schedulingapi.PodGroupPolicy{}, false
}
