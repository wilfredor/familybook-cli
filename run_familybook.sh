#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

HOST="${FAMILYBOOK_HOST:-127.0.0.1}"
PORT="${FAMILYBOOK_PORT:-53682}"
APP_PID=""

# Keep banner URL aligned with optional CLI overrides.
ARGS=("$@")
idx=0
while [[ "${idx}" -lt "${#ARGS[@]}" ]]; do
  arg="${ARGS[$idx]}"
  case "${arg}" in
    --host)
      if [[ $((idx + 1)) -lt "${#ARGS[@]}" ]]; then
        HOST="${ARGS[$((idx + 1))]}"
      fi
      idx=$((idx + 2))
      continue
      ;;
    --host=*)
      HOST="${arg#*=}"
      ;;
    --port)
      if [[ $((idx + 1)) -lt "${#ARGS[@]}" ]]; then
        PORT="${ARGS[$((idx + 1))]}"
      fi
      idx=$((idx + 2))
      continue
      ;;
    --port=*)
      PORT="${arg#*=}"
      ;;
  esac
  idx=$((idx + 1))
done

print_welcome() {
  cat <<'EOF'
+------------------------------------------------------------+
| ######   ###   ##   ##  #### ##      ##   ##  #######  ## |
| ##      ## ##  ### ###   ##  ##      ##   ##  ##   ##  ## |
| ####    #####  ## # ##   ##  ##       ## ##   ##   ##  ## |
| ##      ## ##  ##   ##   ##  ##        ###    ##   ##     |
| ##      ## ##  ##   ##  #### ######     #     #######  ## |
+------------------------------------------------------------+
EOF
  echo
  echo "Familybook local server"
  echo "URL: http://${HOST}:${PORT}"
  echo "Cerrar: escribe 'q' y presiona Enter, o usa Ctrl+C"
  echo
}

stop_server() {
  if [[ -n "${APP_PID}" ]] && kill -0 "${APP_PID}" 2>/dev/null; then
    kill "${APP_PID}" 2>/dev/null || true
    wait "${APP_PID}" 2>/dev/null || true
  fi
}

is_familybook_process() {
  local cmd
  cmd="$(ps -p "$1" -o command= 2>/dev/null || true)"
  [[ "${cmd}" == *"familybook-backend"* ]] \
    || [[ "${cmd}" == *"familybook_app.py"* ]] \
    || [[ "${cmd}" == *"/Familybook.app/Contents/MacOS/familybook_desktop"* ]]
}

check_port_before_start() {
  local listener_pids pid non_familybook
  listener_pids="$(lsof -nP -t -iTCP:"${PORT}" -sTCP:LISTEN 2>/dev/null | tr '\n' ' ')"
  if [[ -z "${listener_pids// }" ]]; then
    return 0
  fi

  non_familybook=""
  for pid in ${listener_pids}; do
    if ! is_familybook_process "${pid}"; then
      non_familybook="${pid}"
      break
    fi
  done

  if [[ -n "${non_familybook}" ]]; then
    local owner_cmd owner_addr
    owner_cmd="$(ps -p "${non_familybook}" -o command= 2>/dev/null || echo "unknown")"
    owner_addr="$(lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN 2>/dev/null | awk 'NR==2 {print $9}')"
    echo
    echo "No se puede iniciar: ${HOST}:${PORT} ya está en uso por otro proceso."
    echo "PID: ${non_familybook}"
    echo "CMD: ${owner_cmd}"
    [[ -n "${owner_addr}" ]] && echo "ADDR: ${owner_addr}"
    echo
    echo "Para liberar el puerto:"
    echo "kill ${non_familybook}"
    echo "o usa otro puerto:"
    echo "./run_familybook.sh --port 53683"
    return 1
  fi

  echo
  echo "Puerto ${HOST}:${PORT} ocupado por Familybook. Reiniciando instancia..."
  for pid in ${listener_pids}; do
    kill "${pid}" 2>/dev/null || true
  done

  # Espera breve para que el puerto quede libre.
  local tries
  tries=0
  while lsof -nP -t -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [[ "${tries}" -ge 20 ]]; then
      echo "No se pudo liberar ${HOST}:${PORT} automáticamente."
      echo "Intenta: pkill -f familybook-backend"
      return 1
    fi
    sleep 0.2
  done

  echo "Instancia anterior detenida. Arrancando de nuevo..."
  echo
  return 0
}

on_interrupt() {
  echo
  echo "Deteniendo Familybook..."
  stop_server
  exit 130
}

trap on_interrupt INT TERM
trap stop_server EXIT

if ! check_port_before_start; then
  exit 1
fi

python3 familybook_app.py "$@" &
APP_PID="$!"

print_welcome

# If stdin is interactive, allow quick quit with 'q' + Enter.
if [[ -t 0 ]]; then
  while kill -0 "${APP_PID}" 2>/dev/null; do
    if IFS= read -r -t 1 line; then
      case "${line}" in
        q|Q|quit|QUIT|exit|EXIT)
          echo "Cerrando Familybook..."
          stop_server
          exit 0
          ;;
      esac
    fi
  done
fi

wait "${APP_PID}"
