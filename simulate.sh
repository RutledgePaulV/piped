#!/usr/bin/env bash
set -e

# simulates the docker in docker setup that you'll find on ci.
# useful for debugging issues unique to that environment.

docker run \
  --rm \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$HOME"/.m2:/root/.m2 \
  -v "$PWD":/usr/src/app \
  --privileged \
  -w=/usr/src/app \
  clojure:openjdk-13-lein \
  lein test