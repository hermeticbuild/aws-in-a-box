#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail

TAG="${1:?release tag is required}"
PREFIX="aws-in-a-box-${TAG#v}"
ARCHIVE="aws-in-a-box-$TAG.tar.gz"

if ! grep -qx "const Version = \"$TAG\"" version.go; then
    echo "version.go must contain the release version $TAG before releasing" >&2
    exit 1
fi

for GOOS in windows linux darwin; do
    for GOARCH in amd64 arm64; do
        GOOS="$GOOS" GOARCH="$GOARCH" CGO_ENABLED=0 \
            go build -ldflags "-w" -trimpath -o "./aws-in-a-box-$GOOS-$GOARCH"
    done
done

git archive --format=tar --prefix="${PREFIX}/" "$TAG" | gzip > "$ARCHIVE"

cat <<EOF
## Add to your \`MODULE.bazel\` file:

\`\`\`starlark
bazel_dep(name = "aws-in-a-box", version = "${TAG#v}")
\`\`\`

EOF
