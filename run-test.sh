#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OS=$(go env GOOS)
ARCH=$(go env GOARCH)

source "${DIR}/run.conf"

# Name of the experiment being run.
ENTITY_NAME="spanner-test"

# Path to the executable.
CMD="${DIR}/build/${OS}-${ARCH}/${ENTITY_NAME}"

# Configurable parameters local to a user.
DB_NAME="ledger"
DB_PATH="projects/${PROJECT_NAME}/instances/${INSTANCE_NAME}/databases/${DB_NAME}"

${CMD} ${DB_PATH}

