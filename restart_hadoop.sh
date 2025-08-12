#!/bin/bash
set -euo pipefail

# =============================================================================
# Hadoop Cluster Restart Helper
# =============================================================================
# Safely restarts HDFS and YARN by invoking the Hadoop sbin scripts on the
# NameNode host. Reads hosts and paths from config.env.
#
# Usage:
#   ./restart_hadoop.sh            # restart both HDFS and YARN
#   ./restart_hadoop.sh --hdfs     # restart only HDFS
#   ./restart_hadoop.sh --yarn     # restart only YARN
#   HADOOP_SERVICE_USER=hdfs ./restart_hadoop.sh
# =============================================================================

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

if [[ -f "$SCRIPT_DIR/config.env" ]]; then
  # shellcheck disable=SC1090
  source "$SCRIPT_DIR/config.env"
else
  echo "❌ config.env not found next to this script ($SCRIPT_DIR)."
  exit 1
fi

ACTION="all"
case "${1:-}" in
  --hdfs) ACTION="hdfs" ;;
  --yarn) ACTION="yarn" ;;
  "" ) : ;; # default all
  *) echo "Usage: $0 [--hdfs|--yarn]" ; exit 2 ;;
esac

NAMENODE_HOST="${HDFS_NAMENODE_HOST:?HDFS_NAMENODE_HOST not set in config.env}"
SERVICE_USER="${HADOOP_SERVICE_USER:-hdfs}"
HADOOP_HOME_REMOTE="${HADOOP_HOME:-/opt/broadcom/tdl/hadoop}"
HADOOP_CONF_DIR_REMOTE="${HADOOP_CONF_DIR:-$HADOOP_HOME_REMOTE/etc/hadoop}"

echo "🌐 Target NN host: $NAMENODE_HOST"
echo "👤 Service user:   $SERVICE_USER"
echo "📦 HADOOP_HOME:    $HADOOP_HOME_REMOTE"
echo "🗂️  HADOOP_CONF:    $HADOOP_CONF_DIR_REMOTE"
echo "🔁 Action:         $ACTION"

read -r -d '' REMOTE_SCRIPT <<'EOS'
set -euo pipefail
run_as() {
  local user="$1"; shift
  if sudo -n -u "$user" -H bash -lc "true" 2>/dev/null; then
    sudo -n -u "$user" -H bash -lc "$*"
  else
    # Fallback: try without sudo (requires PATH/perm)
    bash -lc "$*"
  fi
}

restart_hdfs() {
  echo "🛑 Stopping HDFS..."
  run_as "$SERVICE_USER" "HADOOP_CONF_DIR=$HADOOP_CONF_DIR HADOOP_HOME=$HADOOP_HOME $HADOOP_HOME/sbin/stop-dfs.sh" || true
  sleep 3
  echo "✅ Stopped HDFS"
  echo "🚀 Starting HDFS..."
  run_as "$SERVICE_USER" "HADOOP_CONF_DIR=$HADOOP_CONF_DIR HADOOP_HOME=$HADOOP_HOME $HADOOP_HOME/sbin/start-dfs.sh"
  echo "📋 HDFS report (head):"
  run_as "$SERVICE_USER" "HADOOP_CONF_DIR=$HADOOP_CONF_DIR hdfs dfsadmin -report | head -n 50" || true
}

restart_yarn() {
  echo "🛑 Stopping YARN..."
  run_as "$SERVICE_USER" "HADOOP_CONF_DIR=$HADOOP_CONF_DIR HADOOP_HOME=$HADOOP_HOME $HADOOP_HOME/sbin/stop-yarn.sh" || true
  sleep 3
  echo "✅ Stopped YARN"
  echo "🚀 Starting YARN..."
  run_as "$SERVICE_USER" "HADOOP_CONF_DIR=$HADOOP_CONF_DIR HADOOP_HOME=$HADOOP_HOME $HADOOP_HOME/sbin/start-yarn.sh"
  echo "📋 YARN nodes (head):"
  run_as "$SERVICE_USER" "HADOOP_CONF_DIR=$HADOOP_CONF_DIR yarn node -list 2>/dev/null | head -n 30" || true
}

echo "🔧 Ensuring paths on remote host..."
if [[ ! -x "$HADOOP_HOME/sbin/stop-dfs.sh" ]]; then
  echo "❌ Hadoop sbin scripts not found at $HADOOP_HOME/sbin on $(hostname)"
  exit 3
fi

case "$ACTION" in
  all)
    restart_yarn
    restart_hdfs
    ;;
  hdfs)
    restart_hdfs
    ;;
  yarn)
    restart_yarn
    ;;
esac

echo "✅ Hadoop restart completed on $(hostname)"
EOS

echo "🔐 Connecting to $NAMENODE_HOST to perform restart..."
ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new "gpadmin@$NAMENODE_HOST" \
  SERVICE_USER="$SERVICE_USER" \
  HADOOP_HOME="$HADOOP_HOME_REMOTE" \
  HADOOP_CONF_DIR="$HADOOP_CONF_DIR_REMOTE" \
  ACTION="$ACTION" \
  "bash -lc '$REMOTE_SCRIPT'"

echo "🎉 Done."


