#!/bin/bash

# build the docs the same way docs.rs does, failing on any rustdoc warnings
RUSTDOCFLAGS="--cfg docsrs -D warnings ${RUSTDOCFLAGS}" cargo +nightly doc --all-features --no-deps "$@"
