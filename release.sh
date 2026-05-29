#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail

: "${VERSION:?set VERSION to a tag such as v0.0.72}"

printf 'package main\n\nconst Version = "%s"\n' "$VERSION" > version.go
git add version.go
git commit -m "Update version file to $VERSION"
git tag -a "$VERSION" -m "$VERSION"
git push origin master "$VERSION"
