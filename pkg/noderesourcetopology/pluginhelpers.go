/*
Copyright 2021 The Kubernetes Authors.

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

package noderesourcetopology

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
)

type TopologyInfoMap struct {
	allowNs sets.String
	lock    sync.RWMutex
	info    map[string]*topologyv1alpha1.NodeResourceTopology
}

func NewTopologyInfoMap(allowNs ...string) *TopologyInfoMap {
	return &TopologyInfoMap{
		allowNs: sets.NewString(allowNs...),
		info:    make(map[string]*topologyv1alpha1.NodeResourceTopology),
	}
}

func (tim *TopologyInfoMap) GetAllowedNamespaces() []string {
	return tim.allowNs.UnsortedList()
}

func (tim *TopologyInfoMap) IsAllowed(ns string) bool {
	// empty allowlist: everyone is allowed! this is intentional
	// no lock needed: allowNs is not changing past initialization
	if tim.allowNs.Len() == 0 {
		return true
	}
	return tim.allowNs.Has(ns)
}

func (tim *TopologyInfoMap) TryAdd(obj *topologyv1alpha1.NodeResourceTopology) bool {
	tim.lock.Lock()
	defer tim.lock.Unlock()
	if !tim.IsAllowed(obj.Namespace) {
		return false
	}
	tim.info[obj.Name] = obj
	return true
}

func (tim *TopologyInfoMap) Add(obj *topologyv1alpha1.NodeResourceTopology) {
	tim.lock.Lock()
	defer tim.lock.Unlock()
	tim.info[obj.Name] = obj
}

func (tim *TopologyInfoMap) Remove(obj *topologyv1alpha1.NodeResourceTopology) {
	tim.lock.Lock()
	defer tim.lock.Unlock()
	delete(tim.info, obj.Name)
}

func (tim *TopologyInfoMap) Get(name string) *topologyv1alpha1.NodeResourceTopology {
	tim.lock.RLock()
	defer tim.lock.RUnlock()
	obj, ok := tim.info[name]
	if !ok {
		return nil
	}
	return obj.DeepCopy()
}

func (tim *TopologyInfoMap) Len() int {
	tim.lock.RLock()
	defer tim.lock.RUnlock()
	return len(tim.info)
}

func createNUMANodeList(zones topologyv1alpha1.ZoneList) NUMANodeList {
	nodes := make(NUMANodeList, 0)
	for _, zone := range zones {
		if zone.Type == "Node" {
			var numaID int
			_, err := fmt.Sscanf(zone.Name, "node-%d", &numaID)
			if err != nil {
				klog.ErrorS(nil, "Invalid zone format", "zone", zone.Name)
				continue
			}
			if numaID > 63 || numaID < 0 {
				klog.ErrorS(nil, "Invalid NUMA id range", "numaID", numaID)
				continue
			}
			resources := extractResources(zone)
			nodes = append(nodes, NUMANode{NUMAID: numaID, Resources: resources})
		}
	}
	return nodes
}

func makePodByResourceList(resources *v1.ResourceList) *v1.Pod {
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: *resources,
						Limits:   *resources,
					},
				},
			},
		},
	}
}

func makeResourceListFromZones(zones topologyv1alpha1.ZoneList) v1.ResourceList {
	result := make(v1.ResourceList)
	for _, zone := range zones {
		for _, resInfo := range zone.Resources {
			resQuantity := resInfo.Available
			if quantity, ok := result[v1.ResourceName(resInfo.Name)]; ok {
				resQuantity.Add(quantity)
			}
			result[v1.ResourceName(resInfo.Name)] = resQuantity
		}
	}
	return result
}

func MakeTopologyResInfo(name, capacity, available string) topologyv1alpha1.ResourceInfo {
	return topologyv1alpha1.ResourceInfo{
		Name:      name,
		Capacity:  resource.MustParse(capacity),
		Available: resource.MustParse(available),
	}
}

func makePodByResourceListWithManyContainers(resources *v1.ResourceList, containerCount int) *v1.Pod {
	var containers []v1.Container

	for i := 0; i < containerCount; i++ {
		containers = append(containers, v1.Container{
			Resources: v1.ResourceRequirements{
				Requests: *resources,
				Limits:   *resources,
			},
		})
	}
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: containers,
		},
	}
}

func extractResources(zone topologyv1alpha1.Zone) v1.ResourceList {
	res := make(v1.ResourceList)
	for _, resInfo := range zone.Resources {
		klog.V(5).InfoS("Extract resources for zone", "resName", resInfo.Name, "resAvailable", resInfo.Available)
		res[v1.ResourceName(resInfo.Name)] = resInfo.Available
	}
	return res
}

func newPolicyHandlerMap() PolicyHandlerMap {
	return PolicyHandlerMap{
		topologyv1alpha1.SingleNUMANodePodLevel:       newPodScopedHandler(),
		topologyv1alpha1.SingleNUMANodeContainerLevel: newContainerScopedHandler(),
	}
}
