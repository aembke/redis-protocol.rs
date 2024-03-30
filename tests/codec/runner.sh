#!/bin/bash

TEST_ARGV="$1" docker-compose -f tests/codec/codec-tests.yml run -u $(id -u ${USER}):$(id -g ${USER}) --rm redis-protocol-codec-tests