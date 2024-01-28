#!/bin/bash

# This thread is used by maintainers (i.e. @digama0) to trigger a new release.
# To start the bump, increase the version in Cargo.toml and commit
# Cargo.toml and Cargo.lock. The commit need not be pushed

version=`cargo metadata --format-version 1 \
  | jq '.packages[] | select(.name == "leangz") | .version' -r`
echo version $version
git tag -a v$version -m v$version
git push --tags origin master v$version
