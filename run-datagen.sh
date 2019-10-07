#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OS=$(go env GOOS)
ARCH=$(go env GOARCH)

# Name of the experiment being run.
ENTITY_NAME="spanner-datagen"

# Path to the executable.
CMD="${DIR}/build/${OS}-${ARCH}/${ENTITY_NAME}"

# Configurable parameters local to a user.
PROJECT_NAME="bigtable-test-254214"
INSTANCE_NAME="ledger-instance"
DB_NAME="ledger"
DB_PATH="projects/${PROJECT_NAME}/instances/${INSTANCE_NAME}/databases/${DB_NAME}"

gcloud spanner instances create ${INSTANCE_NAME} \
	--config=regional-us-east4 \
	--description="Ledger" \
	--nodes=3

${CMD} ${DB_PATH}

