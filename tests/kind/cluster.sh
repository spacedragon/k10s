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
  kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  kind create cluster \
    --name "$CLUSTER_NAME" \
    --image "$KIND_IMAGE" \
    --kubeconfig "$KUBECONFIG_PATH" \
    --wait 120s

  awk 'BEGIN { RS="---" } /kind: CustomResourceDefinition/ { print }' \
    "$ROOT_DIR/tests/kind/fixtures.yaml" | \
    kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f -
  kubectl --kubeconfig "$KUBECONFIG_PATH" wait \
    --for=condition=Established crd/widgets.example.k10s.io --timeout=60s
  kubectl --kubeconfig "$KUBECONFIG_PATH" apply -f "$ROOT_DIR/tests/kind/fixtures.yaml"
  kubectl --kubeconfig "$KUBECONFIG_PATH" -n k10s-read \
    rollout status deployment/read-path-web --timeout=120s

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
  kubectl --kubeconfig "$KUBECONFIG_PATH" config use-context k10s-limited >/dev/null
  echo "kind read-path cluster ready: $KUBECONFIG_PATH"
}

down() {
  require kind
  kind delete cluster --name "$CLUSTER_NAME"
  rm -f "$KUBECONFIG_PATH"
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  *) echo "usage: $0 [up|down]" >&2; exit 2 ;;
esac
