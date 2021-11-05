#!/bin/bash

set -xeu

kubectl patch clusterrole system:kube-scheduler --type='json' -p='[{"op": "add", "path": "/rules/0", "value":{ "apiGroups": ["topology.node.k8s.io"], "resources": ["noderesourcetopologies"], "verbs": ["get","list","watch"]}}]'
kubectl patch clusterrole system:kube-scheduler --type='json' -p='[{"op": "add", "path": "/rules/0", "value":{ "apiGroups": [""], "resources": ["configmaps"], "verbs": ["get","list","watch"]}}]'
kubectl patch clusterrole system:kube-scheduler --type='json' -p='[{"op": "add", "path": "/rules/0", "value":{ "apiGroups": ["storage.k8s.io"], "resources": ["storageclasses"], "verbs": ["get","list","watch"]}}]'
