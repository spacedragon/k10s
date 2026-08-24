#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="${K10S_KIND_CLUSTER:-k10s-read-path}"
KUBECONFIG_PATH="${K10S_KIND_KUBECONFIG:-${ROOT_DIR}/tests/kind/.kubeconfig}"
KIND_IMAGE="${K10S_KIND_IMAGE:-kindest/node:v1.36.1}"

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required command is missing: $1" >&2
    exit 1
  }
}

up() {
  require docker
  require kind
  require kubectl
  mkdir -p "$(dirname "$KUBECONFIG_PATH")"
  kind delete cluster --name "$CLUSTER_NAME" \
    --kubeconfig "$KUBECONFIG_PATH" >/dev/null 2>&1 || true
  kind create cluster \
    --name "$CLUSTER_NAME" \
    --image "$KIND_IMAGE" \
    --kubeconfig "$KUBECONFIG_PATH" \
    --wait 120s

  # The kind node cannot reach Docker Hub directly on the self-hosted runner.
  # Fetch the pinned fixture image through skopeo (which honors the runner's
  # proxy), import it into Docker, and preload it into the cluster. Keeping the
  # Docker image around also makes subsequent CI runs fully local.
  if ! docker image inspect busybox:1.36.1 >/dev/null 2>&1; then
    require skopeo
    local busybox_archive
    busybox_archive="${RUNNER_TEMP:-/tmp}/k10s-busybox-1.36.1.tar"
    skopeo copy \
      docker://docker.io/library/busybox:1.36.1 \
      "oci-archive:${busybox_archive}:docker.io/library/busybox:1.36.1"
    docker load --input "$busybox_archive"
  fi
  kind load docker-image busybox:1.36.1 --name "$CLUSTER_NAME"

  awk 'BEGIN { RS="---" } /kind: CustomResourceDefinition/ { print }' \
    "$ROOT_DIR/tests/kind/fixtures.yaml" | \
    kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f -
  kubectl --kubeconfig "$KUBECONFIG_PATH" wait \
    --for=condition=Established crd/widgets.example.k10s.io --timeout=60s
  kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f "$ROOT_DIR/tests/kind/fixtures.yaml"
  kubectl --kubeconfig "$KUBECONFIG_PATH" apply --server-side --field-manager=k10s \
    -f "$ROOT_DIR/tests/kind/operations.yaml"
  kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-read \
    rollout status deployment/read-path-web --timeout=120s
  kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-operations \
    rollout status deployment/operations-web --timeout=120s
  kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-operations \
    wait --for=condition=Ready pod/operations-shell --timeout=120s

  local deployment_uid
  deployment_uid="$(kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-read \
    get deployment/read-path-web -o jsonpath='{.metadata.uid}')"
  kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-read patch \
    event/read-path-web-created --type=merge \
    -p "{\"involvedObject\":{\"uid\":\"${deployment_uid}\"}}" >/dev/null

  local cluster token
  cluster="$(kubectl --kubeconfig "$KUBECONFIG_PATH" config view -o jsonpath='{.contexts[0].context.cluster}')"
  token="$(kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-read create token k10s-reader --duration=2h)"
  kubectl --kubeconfig "$KUBECONFIG_PATH" config set-credentials k10s-reader --token="$token" >/dev/null
  kubectl --kubeconfig "$KUBECONFIG_PATH" config set-context k10s-limited \
    --cluster="$cluster" --user=k10s-reader --namespace=k10s-read >/dev/null
  token="$(kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-operations create token k10s-operator --duration=2h)"
  kubectl --kubeconfig "$KUBECONFIG_PATH" config set-credentials k10s-operator --token="$token" >/dev/null
  kubectl --kubeconfig "$KUBECONFIG_PATH" config set-context k10s-operations \
    --cluster="$cluster" --user=k10s-operator --namespace=k10s-operations >/dev/null
  kubectl --kubeconfig "$KUBECONFIG_PATH" config use-context k10s-limited >/dev/null
  echo "kind read-path cluster ready: $KUBECONFIG_PATH"
}

down() {
  require kind
  local status=0
  kind delete cluster --name "$CLUSTER_NAME" \
    --kubeconfig "$KUBECONFIG_PATH" || status=$?
  rm -f "$KUBECONFIG_PATH"
  return "$status"
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 [up|down]" >&2; exit 2 ;;
esac
