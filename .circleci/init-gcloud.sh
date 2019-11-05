#!/bin/bash
SCRIPT=${BASH_SOURCE[0]}
DIR=$(dirname ${BASH_SOURCE[0]})

source ${DIR}/init.sh

echo $GCLOUD_SERVICE_KEY | gcloud auth activate-service-account --key-file=- || exit 1;
gcloud config set project $GCLOUD_PROJECT || exit 1;
gcloud container clusters get-credentials $CLUSTER --zone us-central1-a --project $GCLOUD_PROJECT || exit 1;
helm init --client-only || exit 1;

exit $?