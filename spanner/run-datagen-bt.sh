#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Configurable parameters local to a user.
INSTANCE_NAME="ledger-instance"
CLUSTER_NAME="ledger"

gcloud bigtable instances create ${INSTANCE_NAME} \
	--cluster="${CLUSTER_NAME}" \
	--cluster-zone="us-east4-c" \
	--display-name="Ledger" \
	--cluster-num-nodes=3 \
	--cluster-storage-type="ssd" \
	--instance-type="PRODUCTION"
