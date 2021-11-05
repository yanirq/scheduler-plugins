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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	apiconfig "sigs.k8s.io/scheduler-plugins/pkg/apis/config"

	topologyv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha1"
	topoclientset "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/clientset/versioned"
	topologyinformers "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/informers/externalversions"
	informerv1alpha1 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/informers/externalversions/topology/v1alpha1"
)

type NUMANode struct {
	NUMAID    int
	Resources v1.ResourceList
}

type NUMANodeList []NUMANode

type nodeResTopologyPlugin struct {
	factory      topologyinformers.SharedInformerFactory
	informer     informerv1alpha1.NodeResourceTopologyInformer
	stopChan     chan struct{}
	topologyInfo *TopologyInfoMap
}

type tmScopeHandler struct {
	filter func(pod *v1.Pod, zones topologyv1alpha1.ZoneList, nodeInfo *framework.NodeInfo) *framework.Status
	score  func(pod *v1.Pod, zones topologyv1alpha1.ZoneList, scorerFn scoreStrategy, resourceToWeightMap resourceToWeightMap) (int64, *framework.Status)
}

func newPodScopedHandler() tmScopeHandler {
	return tmScopeHandler{
		filter: singleNUMAPodLevelHandler,
		score:  podScopeScore,
	}
}

func newContainerScopedHandler() tmScopeHandler {
	return tmScopeHandler{
		filter: singleNUMAContainerLevelHandler,
		score:  containerScopeScore,
	}
}

type PolicyHandlerMap map[topologyv1alpha1.TopologyManagerPolicy]tmScopeHandler

// TopologyMatch plugin which run simplified version of TopologyManager's admit handler
type TopologyMatch struct {
	nodeResTopologyPlugin
	policyHandlers      PolicyHandlerMap
	scorerFn            scoreStrategy
	resourceToWeightMap resourceToWeightMap
}

var _ framework.FilterPlugin = &TopologyMatch{}
var _ framework.ScorePlugin = &TopologyMatch{}
var _ framework.EnqueueExtensions = &TopologyMatch{}

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "NodeResourceTopologyMatch"
)

// Name returns name of the plugin. It is used in logs, etc.
func (tm *TopologyMatch) Name() string {
	return Name
}

// New initializes a new plugin and returns it.
func New(args runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	klog.V(5).InfoS("Creating new TopologyMatch plugin")
	tcfg, ok := args.(*apiconfig.NodeResourceTopologyMatchArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NodeResourceTopologyMatchArgs, got %T", args)
	}

	kubeConfig, err := clientcmd.BuildConfigFromFlags(tcfg.MasterOverride, tcfg.KubeConfigPath)
	if err != nil {
		klog.ErrorS(err, "Cannot create kubeconfig", "masterOverride", tcfg.MasterOverride, "kubeConfigPath", tcfg.KubeConfigPath)
		return nil, err
	}

	topoClient, err := topoclientset.NewForConfig(kubeConfig)
	if err != nil {
		klog.ErrorS(err, "Cannot create clientset for NodeTopologyResource", "kubeConfig", kubeConfig)
		return nil, err
	}

	topologyInformerFactory := topologyinformers.NewSharedInformerFactory(topoClient, 0)
	nodeTopologyInformer := topologyInformerFactory.Topology().V1alpha1().NodeResourceTopologies()
	informer := nodeTopologyInformer.Informer()

	stopper := make(chan struct{})
	topologyInfo := NewTopologyInfoMap(tcfg.Namespaces...)

	if allowed := topologyInfo.GetAllowedNamespaces(); len(allowed) > 0 {
		klog.InfoS("restricting NodeResourceTopology lookup", "namespaces", allowed)
	} else {
		klog.InfoS("cluster-wide NodeResourceTopology lookup")
	}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			nrt, ok := obj.(*topologyv1alpha1.NodeResourceTopology)
			if !ok {
				// TODO: more informative message
				klog.V(5).InfoS("Unexpected object")
				return
			}
			topologyInfo.TryAdd(nrt)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			nrt, ok := newObj.(*topologyv1alpha1.NodeResourceTopology)
			if !ok {
				// TODO: more informative message
				klog.V(5).InfoS("Unexpected object")
				return
			}
			topologyInfo.TryAdd(nrt)
		},
		DeleteFunc: func(obj interface{}) {
			nrt, ok := obj.(*topologyv1alpha1.NodeResourceTopology)
			if !ok {
				// TODO: more informative message
				klog.V(5).InfoS("Unexpected object")
				return
			}
			topologyInfo.Remove(nrt)
		},
	})

	klog.InfoS("Start NodeTopologyInformer")

	go informer.Run(stopper)

	klog.InfoS("Synching NodeTopologyInformer...")
	if !cache.WaitForCacheSync(stopper, informer.HasSynced) {
		return nil, fmt.Errorf("Timed out waiting for caches to sync")
	}
	klog.InfoS("Synched NodeTopologyInformer", "object count", topologyInfo.Len())

	scoringFunction, err := getScoringStrategyFunction(tcfg.ScoringStrategy.Type)
	if err != nil {
		return nil, err
	}

	resToWeightMap := make(resourceToWeightMap)
	for _, resource := range tcfg.ScoringStrategy.Resources {
		resToWeightMap[v1.ResourceName(resource.Name)] = resource.Weight
	}

	topologyMatch := &TopologyMatch{
		nodeResTopologyPlugin: nodeResTopologyPlugin{
			factory:      topologyInformerFactory,
			informer:     nodeTopologyInformer,
			stopChan:     stopper,
			topologyInfo: topologyInfo,
		},
		policyHandlers:      newPolicyHandlerMap(),
		scorerFn:            scoringFunction,
		resourceToWeightMap: resToWeightMap,
	}

	klog.InfoS("Plugin ready")
	return topologyMatch, nil
}

// EventsToRegister returns the possible events that may make a Pod
// failed by this plugin schedulable.
// NOTE: if in-place-update (KEP 1287) gets implemented, then PodUpdate event
// should be registered for this plugin since a Pod update may free up resources
// that make other Pods schedulable.
func (tm *TopologyMatch) EventsToRegister() []framework.ClusterEvent {
	return []framework.ClusterEvent{
		{Resource: framework.Pod, ActionType: framework.Delete},
		{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeAllocatable},
	}
}
