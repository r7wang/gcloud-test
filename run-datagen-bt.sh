#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OS=$(go env GOOS)
ARCH=$(go env GOARCH)

# Name of the experiment being run.
ENTITY_NAME="bigtable-datagen"

# Path to the executable.
CMD="${DIR}/build/${OS}-${ARCH}/${ENTITY_NAME}"

# Configurable parameters local to a user.
PROJECT_NAME="bigtable-test-254214"
INSTANCE_NAME="ledger-instance"
CLUSTER_NAME="ledger"

gcloud bigtable instances create ${INSTANCE_NAME} \
	--cluster="${CLUSTER_NAME}" \
	--cluster-zone="us-east4-c" \
	--display-name="Ledger" \
	--cluster-num-nodes=3 \
	--cluster-storage-type="ssd" \
	--instance-type="PRODUCTION"

${CMD} ${PROJECT_NAME} ${INSTANCE_NAME}
