#!/usr/bin/env bash
# Run autodev under tmux: Temporal dev server and csub broker on the host, the worker inside agentic-sandbox.
# Identity = the account running this + autodev.yaml: the same script serves a personal test instance and
# the service account. Attach: tmux attach -t <deployment>    Stop: tmux kill-session -t <deployment>
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$PWD
PY=$REPO/.pixi/envs/default/bin/python
SANDBOX=${SANDBOX:-$REPO/../agentic-sandbox/scripts}
[ -x "$PY" ] || { echo "run 'pixi install' first"; exit 1; }
[ -x "$SANDBOX/sandbox-run.sh" ] || { echo "agentic-sandbox not found at $SANDBOX (set \$SANDBOX)"; exit 1; }

eval "$("$PY" - <<'EOF'
import os
import shlex
from autodev.config import PROJECT_DIR, load_config
c = load_config()
g = c.github
secret = g.private_key_path if g.auth == "app" else g.token_file
for k, v in {"DEPLOY": c.deployment, "STATE": c.paths.state_dir, "WORK": c.paths.work_root, "SECRET": secret or "",
             "CFG": os.environ.get("AUTODEV_CONFIG") or PROJECT_DIR / "autodev.yaml"}.items():
    print(f"{k}={shlex.quote(str(v))}")
EOF
)"
# The sandbox masks .env and tmux passes no env from this shell, so the worker's GitHub secret must be a file.
[ -n "$SECRET" ] || { echo "set github.token_file (auth: token) or github.private_key_path (auth: app)"; exit 1; }
RUN=/scratch/$USER/$DEPLOY   # node-local: the csub broker's unix socket
mkdir -p "$STATE/logs" "$STATE/bin" "$WORK" "$RUN"

TEMPORAL=$(command -v temporal || echo "$STATE/bin/temporal")
if [ ! -x "$TEMPORAL" ]; then
  curl -sSL "https://temporal.download/cli/archive/latest?platform=linux&arch=amd64" | tar xz -C "$STATE/bin" temporal
fi
if tmux has-session -t "$DEPLOY" 2>/dev/null; then echo "already running: tmux attach -t $DEPLOY"; exit 0; fi

# Temporal gRPC and UI stay on this host's loopback; the sandboxed worker reaches them via the allowlist proxy.
tmux new-session -d -s "$DEPLOY" -n temporal \
  "'$TEMPORAL' server start-dev --db-filename '$STATE/temporal.db' 2>&1 | tee -a '$STATE/logs/temporal.log'"
until "$TEMPORAL" operator cluster health >/dev/null 2>&1; do sleep 1; done
tmux new-window -t "$DEPLOY" -n broker "csub-broker --serve '$RUN/csub.sock' 2>&1 | tee -a '$STATE/logs/broker.log'"

# cwd = $STATE, which sandbox-run.sh binds read-write; the repo (code + pixi env) is read-only.
tmux new-window -t "$DEPLOY" -n worker "cd '$STATE' && '$SANDBOX/sandbox-run.sh' --ro '$REPO' --ro '$CFG' \
  --ro '$SECRET' --rw '$WORK' --rw '$RUN' --allow github.com --allow api.github.com --allow localhost -- \
  env AUTODEV_CONFIG='$CFG' CSUB_TRANSPORT=unix CSUB_SOCKET='$RUN/csub.sock' AUTODEV_TEMPORAL_VIA_PROXY=1 \
  '$PY' -m autodev.cli worker; read -p 'worker exited; press enter'"
"$PY" -m autodev.cli schedule apply

cat <<MSG
autodev '$DEPLOY' running in tmux (windows: temporal, broker, worker).
  attach:      tmux attach -t $DEPLOY
  logs:        tail -f $STATE/logs/worker.log
  Temporal UI: http://localhost:8233 (ssh -L 8233:localhost:8233 $(hostname -f))
  trigger:     add the label '$DEPLOY:go' to an issue
MSG
