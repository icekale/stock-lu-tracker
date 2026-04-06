#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-icekale/stock-lu-tracker}"
VERSION_TAG="${1:-}"

if [[ -z "${VERSION_TAG}" ]]; then
  VERSION_TAG="v$(node -p "require('./package.json').version")"
fi

echo "Publishing ${IMAGE_NAME}:${VERSION_TAG} and ${IMAGE_NAME}:latest"

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --provenance=false \
  --tag "${IMAGE_NAME}:${VERSION_TAG}" \
  --tag "${IMAGE_NAME}:latest" \
  --push \
  .

for tag in "${IMAGE_NAME}:${VERSION_TAG}" "${IMAGE_NAME}:latest"; do
  echo
  echo "Verifying ${tag}"
  output="$(docker buildx imagetools inspect "${tag}")"
  echo "${output}"
  grep -q "linux/amd64" <<<"${output}"
  grep -q "linux/arm64" <<<"${output}"
done

echo
echo "Done."
