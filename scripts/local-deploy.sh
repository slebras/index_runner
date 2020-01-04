#!/bin/sh
set -e
# show the commands we execute
set -o xtrace
export IMAGE_NAME="kbase/index_runner2:1.3.0"
sh hooks/build
docker push $IMAGE_NAME
