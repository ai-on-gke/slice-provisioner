#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -e

# Determine the directory of this script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

CLUSTER_NAME="slice-provisioner-test-e2e"
IMAGE_TAG="latest"
NAMESPACE="slice-provisioner-system"

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --cluster) CLUSTER_NAME="$2"; shift ;;
        --namespace) NAMESPACE="$2"; shift ;;
        --image-tag) IMAGE_TAG="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

echo ""
echo "--- Building and deploying slice-provisioner ---"
echo "--- Building and loading the provisioner Docker image ---"
make -C $SCRIPT_DIR docker-build IMG=slice-provisioner:$IMAGE_TAG
kind load docker-image slice-provisioner:$IMAGE_TAG --name $CLUSTER_NAME

echo "--- Deploying the Helm chart ---"
helm upgrade --install slice-provisioner $SCRIPT_DIR/charts/slice-provisioner --namespace $NAMESPACE --create-namespace --set image.tag="$IMAGE_TAG"

echo "--- Waiting for slice-provisioner to be ready ---"
kubectl wait --for=condition=Available=True deployment/slice-provisioner -n $NAMESPACE --timeout=120s

echo "---  Waiting for webhook endpointslice to be ready ---"
kubectl wait --for=jsonpath='{.endpoints[0].conditions.ready}'=true \
  endpointslice -l kubernetes.io/service-name=slice-provisioner-webhook-service \
  -n $NAMESPACE --timeout=120s

echo "--- Slice provisioner deployed successfully ---"
