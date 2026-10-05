#!/usr/bin/env bash
# Start the PoC under tmux: Temporal dev server, zarr-doctor worker, poll schedule.
# Re-attach: tmux attach -t zarr-doctor    Stop: tmux kill-session -t zarr-doctor
set -euo pipefail
cd "$(dirname "$0")/.."

STATE_DIR=$(pixi run -q python -c 'from zarr_doctor.config import load_config; print(load_config().state_dir)')
LOGS="$STATE_DIR/logs"
mkdir -p "$LOGS" "$STATE_DIR/bin"

TEMPORAL=$(command -v temporal || echo "$STATE_DIR/bin/temporal")
if [ ! -x "$TEMPORAL" ]; then
  echo "downloading Temporal CLI to $TEMPORAL"
  curl -sSL "https://temporal.download/cli/archive/latest?platform=linux&arch=amd64" | tar xz -C "$STATE_DIR/bin" temporal
fi

SESSION=zarr-doctor
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "tmux session $SESSION already running; attach with: tmux attach -t $SESSION"; exit 0
fi

# gRPC stays on localhost; the UI listens on all interfaces so it is reachable at http://$(hostname -f):8233
tmux new-session -d -s "$SESSION" -n temporal \
  "'$TEMPORAL' server start-dev --db-filename '$STATE_DIR/temporal.db' --ui-ip 0.0.0.0 2>&1 | tee -a '$LOGS/temporal.log'"
until "$TEMPORAL" operator cluster health >/dev/null 2>&1; do sleep 1; done

tmux new-window -t "$SESSION" -n worker "pixi run zarr-doctor worker; read -p 'worker exited; press enter'"
pixi run zarr-doctor schedule apply

cat <<MSG
Running in tmux session '$SESSION' (windows: temporal, worker).
  attach:     tmux attach -t $SESSION
  logs:       tail -f $LOGS/worker.log $LOGS/temporal.log
  Temporal UI: http://$(hostname -f):8233
  discover:   pixi run zarr-doctor discover --max-new 10
MSG
