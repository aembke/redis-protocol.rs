#!/bin/bash

# boot all the redis servers and start a bash shell on a new container
docker-compose -f tests/codec/codec-tests.yml \
  -f tests/codec/base.yml run -u $(id -u ${USER}):$(id -g ${USER}) --rm debug