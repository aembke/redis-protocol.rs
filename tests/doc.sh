#!/bin/bash

cargo +nightly rustdoc --features "std bytes codec convert resp2 resp3" "$@" -- --cfg docsrs