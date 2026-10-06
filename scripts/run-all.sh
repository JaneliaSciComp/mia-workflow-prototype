#!/usr/bin/env bash
# Start the PoC under tmux: Prefect server, zarr-doctor worker (serves both deployments), zombie automation.
# Re-attach: tmux attach -t zarr-doctor-prefect    Stop: tmux kill-session -t zarr-doctor-prefect
set -euo pipefail
cd "$(dirname "$0")/.."

STATE_DIR=$(pixi run -q python -c 'from zarr_doctor.config import load_config; print(load_config().state_dir)')
API_URL=$(pixi run -q python -c 'from zarr_doctor.config import load_config; print(load_config().prefect.api_url)')
PORT=$(python3 -c "import sys, urllib.parse as u; print(u.urlparse(sys.argv[1]).port or 4200)" "$API_URL")
LOGS="$STATE_DIR/logs"
# The server's SQLite DB must be on LOCAL disk: on NFS (/groups) it fails with "database is locked" and 500s.
# /scratch is node-local but cleaned periodically; production should use Postgres.
PREFECT_HOME=${PREFECT_HOME:-/scratch/$USER/zarr-doctor/prefect}
mkdir -p "$LOGS" "$PREFECT_HOME"

SESSION=zarr-doctor-prefect
if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "tmux session $SESSION already running; attach with: tmux attach -t $SESSION"; exit 0
fi

# The UI and API share one port; binding 0.0.0.0 makes the UI
# reachable at http://$(hostname -f):$PORT — and the (unauthenticated) API with it.
# PREFECT_UI_API_URL: the URL the browser uses for API calls; default would be 127.0.0.1 (the viewer's own machine).
# PREFECT_CLI_PROMPT=false: otherwise `server start` can stop at an interactive "profile URL mismatch" prompt.
UI_API_URL="http://$(hostname -f):$PORT/api"
tmux new-session -d -s "$SESSION" -n server \
  "PREFECT_HOME='$PREFECT_HOME' PREFECT_API_URL='$API_URL' PREFECT_UI_API_URL='$UI_API_URL' PREFECT_CLI_PROMPT=false pixi run prefect server start --host 0.0.0.0 --port $PORT 2>&1 | tee -a '$LOGS/prefect-server.log'"
until curl -sf "$API_URL/health" >/dev/null; do sleep 1; done

pixi run zarr-doctor setup
tmux new-window -t "$SESSION" -n worker "pixi run zarr-doctor worker; read -p 'worker exited; press enter'"

cat <<MSG
Running in tmux session '$SESSION' (windows: server, worker).
  attach:     tmux attach -t $SESSION
  logs:       tail -f $LOGS/worker.log $LOGS/prefect-server.log
  Prefect UI: http://$(hostname -f):$PORT
  discover:   pixi run zarr-doctor discover --max-new 10
MSG
