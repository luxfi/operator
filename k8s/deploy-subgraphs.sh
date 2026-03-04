#!/usr/bin/env bash
set -euo pipefail

# Deploy V2 and V3 subgraphs to graph-node
# Prerequisites: graph-node + graph-ipfs running in lux-explorer namespace
# Usage: ./deploy-subgraphs.sh [--v2-only|--v3-only] [--version vX.Y.Z]
#
# Port-forwards are created automatically and cleaned up on exit.

GRAPH_ADMIN_PORT=18020
IPFS_PORT=15001
VERSION="${VERSION:-v3.4.0}"
DEPLOY_V2=true
DEPLOY_V3=true

V2_DIR="${V2_DIR:-$HOME/work/lux/uni-v2-subgraph}"
V3_DIR="${V3_DIR:-$HOME/work/lux/uni-v3-subgraph}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --v2-only) DEPLOY_V3=false; shift ;;
    --v3-only) DEPLOY_V2=false; shift ;;
    --version) VERSION="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

cleanup() {
  echo "[cleanup] Killing port-forwards..."
  kill "$PF_ADMIN" 2>/dev/null || true
  kill "$PF_IPFS" 2>/dev/null || true
}
trap cleanup EXIT

echo "[subgraphs] Setting up port-forwards to graph-node..."
kubectl --context do-sfo3-lux-k8s port-forward -n lux-explorer svc/graph-node "$GRAPH_ADMIN_PORT:8020" &>/dev/null &
PF_ADMIN=$!
kubectl --context do-sfo3-lux-k8s port-forward -n lux-explorer svc/graph-ipfs "$IPFS_PORT:5001" &>/dev/null &
PF_IPFS=$!
sleep 2

GRAPH_NODE="http://localhost:$GRAPH_ADMIN_PORT"
IPFS_URL="http://localhost:$IPFS_PORT"

if $DEPLOY_V3; then
  echo "[subgraphs] Building V3 subgraph..."
  cd "$V3_DIR"
  npm run codegen && npm run build

  echo "[subgraphs] Deploying luxfi/uniswap-v3 ($VERSION)..."
  npx graph remove --node "$GRAPH_NODE" luxfi/uniswap-v3 2>/dev/null || true
  npx graph create --node "$GRAPH_NODE" luxfi/uniswap-v3
  npx graph deploy --node "$GRAPH_NODE" --ipfs "$IPFS_URL" luxfi/uniswap-v3 --version-label "$VERSION"
  echo "[subgraphs] V3 deployed"
fi

if $DEPLOY_V2; then
  echo "[subgraphs] Building V2 subgraph..."
  cd "$V2_DIR"
  npm run codegen && npm run build

  echo "[subgraphs] Deploying luxfi/uniswap-v2 ($VERSION)..."
  npx graph remove --node "$GRAPH_NODE" luxfi/uniswap-v2 2>/dev/null || true
  npx graph create --node "$GRAPH_NODE" luxfi/uniswap-v2
  npx graph deploy --node "$GRAPH_NODE" --ipfs "$IPFS_URL" luxfi/uniswap-v2 --version-label "$VERSION"
  echo "[subgraphs] V2 deployed"
fi

echo
echo "[subgraphs] Checking sync status..."
sleep 3
curl -s "http://localhost:${GRAPH_ADMIN_PORT%0}0/subgraphs" -X POST \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ indexingStatuses { subgraph synced health chains { network latestBlock { number } chainHeadBlock { number } } } }"}' | \
  python3 -m json.tool 2>/dev/null || echo "(query failed — check manually)"

echo "[subgraphs] done"
