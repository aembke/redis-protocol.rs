#!/bin/bash

cargo +nightly rustdoc --features "std bytes codec convert" "$@" -- --cfg docsrs