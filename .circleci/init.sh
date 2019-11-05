#!/bin/bash
SCRIPT=${BASH_SOURCE[0]}
DIR=$(dirname ${BASH_SOURCE[0]})

export PATH=$PATH:./google-cloud-sdk/bin:/tmp

export GCLOUD_PROJECT=videocoin-network
export ENV=snb
export CLUSTER=snb

if [ "$CIRCLE_BRANCH" == "sandbox" ]; then
    export GCLOUD_PROJECT=videocoin-network
    export ENV=snb
    export CLUSTER=snb
fi

if [ "$CIRCLE_BRANCH" == "develop" ]; then
    export GCLOUD_PROJECT=videocoin-network
    export ENV=dev
    export CLUSTER=dev
fi