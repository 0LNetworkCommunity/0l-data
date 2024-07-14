#!/bin/bash

COMMIT_HASH="$(git describe --always --abbrev=64)"
CONTAINER_REGISTRY="ghcr.io/minaxolone/0l-data"
OL_DATA_IMAGE_TAG="$CONTAINER_REGISTRY:$COMMIT_HASH"

docker build . \
  -t $OL_DATA_IMAGE_TAG \
  -f ./Dockerfile \
  --target ol-data
