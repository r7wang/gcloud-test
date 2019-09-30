#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OS=$(go env GOOS)
ARCH=$(go env GOARCH)

# Name of the experiment being run.
ENTITY_NAME="leaderboard"

# Path to the executable.
CMD="${DIR}/${ENTITY_NAME}/build/${OS}-${ARCH}/${ENTITY_NAME}"

# Configurable parameters local to a user.
PROJECT_NAME="bigtable-test-254214"
INSTANCE_NAME="test-instance"
DB_PATH="projects/${PROJECT_NAME}/instances/${INSTANCE_NAME}/databases/${ENTITY_NAME}"

gcloud spanner instances create ${INSTANCE_NAME} --config=regional-us-east1 --description="Test Instance" --nodes=1

${CMD} createdatabase ${DB_PATH}
${CMD} insertplayers ${DB_PATH}
${CMD} insertscores ${DB_PATH}
${CMD} query ${DB_PATH}
${CMD} querywithtimespan ${DB_PATH} 168

