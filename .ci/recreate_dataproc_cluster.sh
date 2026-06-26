#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

if [ $# -lt 4 ]; then
  echo "Error: Missing required arguments." >&2
  echo "Usage: $0 <project_id> <region> <image_version> <cluster_name>" >&2
  exit 1
fi

PROJECT_ID="$1"
REGION="$2"
IMAGE_VERSION="$3"
CLUSTER_NAME="$4"

SERVICE_ACCOUNT="toolbox-identity@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=========================================================="
echo "Recreating Dataproc cluster in project: ${PROJECT_ID}"
echo "Region:          ${REGION}"
echo "Image Version:   ${IMAGE_VERSION}"
echo "Cluster Name:    ${CLUSTER_NAME}"
echo "Service Account: ${SERVICE_ACCOUNT}"
echo "=========================================================="

# Check if the cluster exists, capturing stdout and stderr to distinguish NOT_FOUND from other errors
echo "Checking if cluster '${CLUSTER_NAME}' exists..."
set +e
DESCRIBE_OUT=$(gcloud dataproc clusters describe "${CLUSTER_NAME}" --region="${REGION}" --project="${PROJECT_ID}" 2>&1)
DESCRIBE_STATUS=$?
set -e

if [ ${DESCRIBE_STATUS} -eq 0 ]; then
  echo "Cluster '${CLUSTER_NAME}' exists. Deleting it..."
  gcloud dataproc clusters delete "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --project="${PROJECT_ID}" \
    --quiet
  echo "Cluster '${CLUSTER_NAME}' deleted successfully."
elif echo "${DESCRIBE_OUT}" | grep -q "NOT_FOUND"; then
  echo "Cluster '${CLUSTER_NAME}' does not exist. Skipping deletion."
else
  echo "Error querying cluster existence: ${DESCRIBE_OUT}" >&2
  exit ${DESCRIBE_STATUS}
fi

# Create the cluster
echo "Creating Dataproc cluster '${CLUSTER_NAME}'..."
gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --image-version="${IMAGE_VERSION}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --scopes=cloud-platform \
  --no-address \
  --network=default \
  --master-machine-type=n4-standard-2 \
  --worker-machine-type=n4-standard-2 \
  --num-workers=2

echo "Cluster '${CLUSTER_NAME}' created successfully."
