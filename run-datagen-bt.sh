#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OS=$(go env GOOS)
ARCH=$(go env GOARCH)

source "${DIR}/run.conf"

# Name of the experiment being run.
ENTITY_NAME="bigtable-datagen"

# Path to the executable.
CMD="${DIR}/build/${OS}-${ARCH}/${ENTITY_NAME}"

# Configurable parameters local to a user.
CLUSTER_NAME="ledger"

gcloud bigtable instances create ${INSTANCE_NAME} \
	--cluster="${CLUSTER_NAME}" \
	--cluster-zone="us-east4-c" \
	--display-name="Ledger" \
	--cluster-num-nodes=3 \
	--cluster-storage-type="ssd" \
	--instance-type="PRODUCTION"

${CMD} ${PROJECT_NAME} ${INSTANCE_NAME}
