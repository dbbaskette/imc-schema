#!/bin/bash
# =============================================================================
# Setup Local Hadoop Client Configuration
# =============================================================================
# This script creates the necessary Hadoop configuration files to allow
# the local hdfs command to connect to your remote HDFS cluster
# =============================================================================

set -euo pipefail

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if config.env exists
if [[ ! -f "config.env" ]]; then
    log_error "config.env not found! Please ensure it exists in the current directory."
    exit 1
fi

# Source the configuration
source config.env

echo -e "${BLUE}🔧 Setting up Hadoop Client Configuration${NC}"
echo "=============================================="
echo

log_info "Using HDFS cluster: ${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"

# Determine Hadoop configuration directory
HADOOP_CONF_DIR=""
if command -v hadoop &> /dev/null; then
    # Try to find existing Hadoop configuration
    HADOOP_HOME=$(dirname $(dirname $(which hadoop)))
    POTENTIAL_CONF_DIRS=(
        "$HADOOP_HOME/etc/hadoop"
        "$HADOOP_HOME/conf"
        "/opt/homebrew/etc/hadoop"
        "/usr/local/etc/hadoop"
        "$HOME/.hadoop/conf"
    )
    
    for dir in "${POTENTIAL_CONF_DIRS[@]}"; do
        if [[ -d "$dir" ]]; then
            HADOOP_CONF_DIR="$dir"
            break
        fi
    done
fi

# If no existing config directory found, create one
if [[ -z "$HADOOP_CONF_DIR" ]]; then
    HADOOP_CONF_DIR="$HOME/.hadoop/conf"
    log_info "Creating Hadoop configuration directory: $HADOOP_CONF_DIR"
    mkdir -p "$HADOOP_CONF_DIR"
else
    log_info "Using existing Hadoop configuration directory: $HADOOP_CONF_DIR"
fi

# Backup existing configuration if it exists
if [[ -f "$HADOOP_CONF_DIR/core-site.xml" ]]; then
    log_warning "Backing up existing configuration..."
    cp "$HADOOP_CONF_DIR/core-site.xml" "$HADOOP_CONF_DIR/core-site.xml.backup.$(date +%Y%m%d_%H%M%S)"
fi

if [[ -f "$HADOOP_CONF_DIR/hdfs-site.xml" ]]; then
    cp "$HADOOP_CONF_DIR/hdfs-site.xml" "$HADOOP_CONF_DIR/hdfs-site.xml.backup.$(date +%Y%m%d_%H%M%S)"
fi

# Create core-site.xml
log_info "Creating core-site.xml..."
cat > "$HADOOP_CONF_DIR/core-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- Default file system -->
    <property>
        <name>fs.defaultFS</name>
        <value>${HDFS_PROTOCOL}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}</value>
        <description>The name of the default file system</description>
    </property>
    
    <!-- Hadoop temporary directory -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/hadoop-\${user.name}</value>
        <description>A base for other temporary directories</description>
    </property>
    
    <!-- Security settings -->
    <property>
        <name>hadoop.security.authentication</name>
        <value>simple</value>
    </property>
    
    <!-- Connection timeout -->
    <property>
        <name>ipc.client.connect.timeout</name>
        <value>20000</value>
    </property>
    
    <property>
        <name>ipc.client.connect.max.retries</name>
        <value>10</value>
    </property>
</configuration>
EOF

# Create hdfs-site.xml
log_info "Creating hdfs-site.xml..."
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- NameNode settings -->
    <property>
        <name>dfs.nameservices</name>
        <value>hdfs-cluster</value>
    </property>
    
    <property>
        <name>dfs.ha.namenodes.hdfs-cluster</name>
        <value>nn1</value>
    </property>
    
    <property>
        <name>dfs.namenode.rpc-address.hdfs-cluster.nn1</name>
        <value>${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}</value>
    </property>
    
    <!-- Client settings -->
    <property>
        <name>dfs.client.failover.proxy.provider.hdfs-cluster</name>
        <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
    </property>
    
    <!-- Replication factor for client writes -->
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    
    <!-- Block size -->
    <property>
        <name>dfs.blocksize</name>
        <value>134217728</value>
    </property>
</configuration>
EOF

# Set HADOOP_CONF_DIR environment variable
log_info "Setting up environment variables..."

# Create a script to set environment variables
cat > "$HOME/.hadoop_env" << EOF
# Hadoop Client Configuration
export HADOOP_CONF_DIR="$HADOOP_CONF_DIR"
export HDFS_NAMENODE_HOST="$HDFS_NAMENODE_HOST"
export HDFS_NAMENODE_PORT="$HDFS_NAMENODE_PORT"

# Add this to your shell profile (.bashrc, .zshrc, etc.)
# source ~/.hadoop_env
EOF

# Set for current session
export HADOOP_CONF_DIR="$HADOOP_CONF_DIR"

log_success "Hadoop client configuration created!"
echo
echo "📁 Configuration files created:"
echo "  • $HADOOP_CONF_DIR/core-site.xml"
echo "  • $HADOOP_CONF_DIR/hdfs-site.xml"
echo "  • $HOME/.hadoop_env"
echo
echo "🔧 Environment setup:"
echo "  • HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "  • Target cluster: ${HDFS_PROTOCOL}://${HDFS_NAMENODE_HOST}:${HDFS_NAMENODE_PORT}"
echo

# Test the connection
log_info "Testing HDFS connection..."
echo

if hdfs dfs -ls / &>/dev/null; then
    log_success "✅ HDFS connection successful!"
    echo
    log_info "Available directories:"
    hdfs dfs -ls / 2>/dev/null | head -10
else
    log_warning "⚠️  HDFS connection failed. This might be due to:"
    echo "    - Network connectivity issues"
    echo "    - Firewall blocking port $HDFS_NAMENODE_PORT"
    echo "    - NameNode not running"
    echo "    - Authentication requirements"
    echo
    log_info "You can test manually with:"
    echo "    export HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
    echo "    hdfs dfs -ls /"
fi

echo
echo "🚀 Next steps:"
echo "  1. Add to your shell profile:"
echo "     echo 'source ~/.hadoop_env' >> ~/.zshrc"
echo "  2. Test telemetry data access:"
echo "     hdfs dfs -ls $HDFS_TELEMETRY_BASE_PATH-v2/"
echo "  3. Run consolidation:"
echo "     ./consolidate_telemetry.sh --date 2025-08-15 --dry-run"
