#!/bin/bash
# =============================================================================
# Setup Consolidation Environment - Best Practices for macOS
# =============================================================================

set -euo pipefail

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}🚀 Setting up Parquet Consolidation Environment${NC}"
echo "================================================"
echo

# Step 1: Create virtual environment
echo -e "${YELLOW}📦 Step 1: Creating virtual environment...${NC}"
if [[ -d "venv-consolidation" ]]; then
    echo "  ✅ Virtual environment already exists"
else
    python3 -m venv venv-consolidation
    echo -e "  ${GREEN}✅ Virtual environment created${NC}"
fi

echo -e "  📁 Location: $(pwd)/venv-consolidation"
echo

# Step 2: Activate and setup
echo -e "${YELLOW}⚡ Step 2: Activating environment and upgrading pip...${NC}"
source venv-consolidation/bin/activate

echo "  🔍 Python: $(which python)"
echo "  🔍 Pip: $(which pip)"

# Upgrade pip
pip install --upgrade pip
echo -e "  ${GREEN}✅ Pip upgraded${NC}"
echo

# Step 3: Install dependencies
echo -e "${YELLOW}📚 Step 3: Installing consolidation dependencies...${NC}"
echo "  Installing pandas..."
pip install pandas

echo "  Installing pyarrow..."
pip install pyarrow

echo "  Installing hdfs3..."
pip install hdfs3 || {
    echo "  ⚠️  hdfs3 failed, trying alternative..."
    pip install pydoop || echo "  ⚠️  Using subprocess fallback for HDFS"
}

echo "  Installing optional dependencies..."
pip install tqdm colorlog

echo -e "  ${GREEN}✅ All dependencies installed${NC}"
echo

# Step 4: Verify installation
echo -e "${YELLOW}🔍 Step 4: Verifying installation...${NC}"
python3 -c "
import pandas as pd
import pyarrow as pa
print('  ✅ pandas:', pd.__version__)
print('  ✅ pyarrow:', pa.__version__)

try:
    import hdfs3
    print('  ✅ hdfs3: available')
except ImportError:
    print('  ⚠️  hdfs3: not available (will use subprocess)')

print('  ✅ All core packages working!')
"

echo
echo -e "${GREEN}🎉 Setup Complete!${NC}"
echo
echo "📋 Usage Instructions:"
echo "  🔧 Activate environment: source venv-consolidation/bin/activate"
echo "  🔧 Run consolidation: ./consolidate_telemetry.sh --date 2025-08-15 --dry-run"
echo "  🔧 Deactivate: deactivate"
echo "  🗑️  Remove environment: rm -rf venv-consolidation/"
echo
echo "📌 The environment is currently ACTIVE for this session."
echo "   Run 'deactivate' to exit, or just close this terminal."
