#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${DIR}/run.conf"

gcloud bigtable instances delete "${INSTANCE_NAME}" --quiet
