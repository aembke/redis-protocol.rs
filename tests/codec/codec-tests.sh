#!/bin/bash

cargo test --features "codec resp2 resp3 bytes convert decode-logs" --release --lib --tests -- --test-threads=1 "$@"