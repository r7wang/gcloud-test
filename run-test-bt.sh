#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OS=$(go env GOOS)
ARCH=$(go env GOARCH)

source "${DIR}/run.conf"

# Name of the experiment being run.
ENTITY_NAME="bigtable-test"

# Path to the executable.
CMD="${DIR}/build/${OS}-${ARCH}/${ENTITY_NAME}"

${CMD} ${PROJECT_NAME} ${INSTANCE_NAME}

