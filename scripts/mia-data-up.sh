#!/usr/bin/env bash
# ponytail: this duplicates much of autodev-up.sh (config vars, broker, sandboxed worker); merge when a third app appears.
# Run mia-data under tmux: csub broker on the host, the worker inside agentic-sandbox. It never starts Temporal;
# start autodev first (scripts/autodev-up.sh owns the shared dev server) and this script uses its own namespace.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$PWD
PY=$REPO/.pixi/envs/default/bin/python
SANDBOX=${SANDBOX:-$REPO/../agentic-sandbox/scripts}
[ -x "$PY" ] || { echo "run 'pixi install' first"; exit 1; }
[ -x "$SANDBOX/sandbox-run.sh" ] || { echo "agentic-sandbox not found at $SANDBOX (set \$SANDBOX)"; exit 1; }

vars=$("$PY" - <<'PYEOF'
import os
import shlex
import sys
from pathlib import Path
from mia_data.config import PROJECT_DIR, load_config
c = load_config()
g = c.github
secret = g.private_key_path if g.auth == "app" else g.token_file
vals = {"DEPLOY": c.deployment, "STATE": Path(c.paths.state_dir).resolve(), "WORK": Path(c.paths.work_root).resolve(),
        "STORE": Path(c.storage.root).resolve(), "HARNESS": Path(c.harness.dir).resolve(),
        "SECRET": Path(secret).resolve() if secret else "",
        "CFG": Path(os.environ.get("MIA_DATA_CONFIG") or PROJECT_DIR / "mia_data.yaml").resolve(),
        "TEMPORAL_ADDRESS": c.temporal.address, "NS": c.temporal.namespace}
for k, v in vals.items():
    if "'" in str(v):  # these go inside single quotes in tmux command strings
        sys.exit(f"mia-data-up.sh: {k} contains a single quote: {v}")
    print(f"{k}={shlex.quote(str(v))}")
PYEOF
)
eval "$vars"
# The sandbox masks .env and tmux passes no env from this shell, so the worker's GitHub secret must be a file.
[ -n "$SECRET" ] || { echo "set github.token_file (auth: token) or github.private_key_path (auth: app)"; exit 1; }
BROKER=$REPO/.pixi/envs/default/bin/csub-broker
[ -x "$BROKER" ] || { echo "csub-broker not found at $BROKER (run 'pixi install')"; exit 1; }
RUN=/scratch/$USER/$DEPLOY   # node-local: the csub broker's unix socket
HOST=$STATE-host
mkdir -p "$STATE/logs" "$HOST/logs" "$WORK" "$STORE" "$RUN"

TEMPORAL=${TEMPORAL:-$(command -v temporal || echo "$HOST/bin/temporal")}
if [ ! -x "$TEMPORAL" ]; then
  mkdir -p "$HOST/bin"
  curl -sSL "https://temporal.download/cli/archive/latest?platform=linux&arch=amd64" | tar xz -C "$HOST/bin" temporal
fi
if tmux has-session -t "$DEPLOY" 2>/dev/null; then echo "already running: tmux attach -t $DEPLOY"; exit 0; fi
"$TEMPORAL" operator cluster health --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1 \
  || { echo "no healthy Temporal at $TEMPORAL_ADDRESS: start it with scripts/autodev-up.sh first"; exit 1; }
"$TEMPORAL" operator namespace describe -n "$NS" --address "$TEMPORAL_ADDRESS" >/dev/null 2>&1 \
  || "$TEMPORAL" operator namespace create -n "$NS" --address "$TEMPORAL_ADDRESS"

tmux new-session -d -s "$DEPLOY" -n broker "'$BROKER' --serve '$RUN/csub.sock' 2>&1 | tee -a '$HOST/logs/broker.log'"
# cwd = $STATE, which sandbox-run.sh binds read-write; the repo (code + pixi env), harness and secret are read-only.
tmux new-window -t "$DEPLOY" -n worker "cd '$STATE' && '$SANDBOX/sandbox-run.sh' --ro '$REPO' --ro '$CFG' \
  --ro '$SECRET' --ro '$HARNESS' --rw '$WORK' --rw '$STORE' --rw '$RUN' \
  --allow github.com --allow api.github.com --allow localhost -- \
  env MIA_DATA_CONFIG='$CFG' CSUB_TRANSPORT=unix CSUB_SOCKET='$RUN/csub.sock' MIA_DATA_TEMPORAL_VIA_PROXY=1 \
  '$PY' -m mia_data.cli worker; read -p 'worker exited; press enter'"
"$PY" -m mia_data.cli schedule apply

cat <<MSG
mia-data '$DEPLOY' running in tmux (windows: broker, worker; Temporal is shared, namespace $NS).
  attach:      tmux attach -t $DEPLOY
  logs:        tmux attach -t $DEPLOY   (broker log: $HOST/logs/broker.log)
  Temporal UI: http://localhost:8233 (ssh -L 8233:localhost:8233 $(hostname -f))
  stop:        tmux kill-session -t $DEPLOY
  trigger:     add the label 'data:go' to a dataset issue
MSG
