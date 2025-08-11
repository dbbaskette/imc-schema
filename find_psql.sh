#!/bin/bash

echo "=== PostgreSQL/libpq Installation Diagnostic ==="
echo "Date: $(date)"
echo ""

echo "🔍 Searching for psql installation..."
echo ""

# Check if libpq is installed via Homebrew
echo "📦 Homebrew libpq status:"
if brew list libpq >/dev/null 2>&1; then
    echo "✅ libpq is installed via Homebrew"
    
    # Get libpq prefix
    LIBPQ_PREFIX=$(brew --prefix libpq 2>/dev/null)
    echo "   Prefix: $LIBPQ_PREFIX"
    
    # List libpq files
    echo "   Files installed:"
    brew list libpq | grep -E "(bin/|psql)" | head -10
    
else
    echo "❌ libpq not found via Homebrew"
fi

echo ""
echo "🔍 Searching for psql in common locations:"

SEARCH_PATHS=(
    "/opt/homebrew/bin/psql"
    "/opt/homebrew/opt/libpq/bin/psql" 
    "/usr/local/bin/psql"
    "/usr/local/opt/libpq/bin/psql"
    "$(brew --prefix)/bin/psql"
    "$(brew --prefix libpq 2>/dev/null)/bin/psql"
)

for path in "${SEARCH_PATHS[@]}"; do
    if [ -x "$path" ]; then
        echo "✅ Found: $path"
        echo "   Version: $($path --version 2>/dev/null | head -1)"
    else
        echo "❌ Not found: $path"
    fi
done

echo ""
echo "🔍 System-wide psql search:"
if command -v psql >/dev/null 2>&1; then
    echo "✅ psql is in PATH: $(which psql)"
    echo "   Version: $(psql --version)"
else
    echo "❌ psql not found in PATH"
fi

echo ""
echo "📂 Current PATH:"
echo "$PATH" | tr ':' '\n' | nl

echo ""
echo "🏠 Home directory shell files:"
ls -la ~/.zshrc ~/.bash_profile ~/.bashrc 2>/dev/null | grep -E "\.(zshrc|bash_profile|bashrc)$"

echo ""
echo "🔧 Architecture: $(uname -m)"
echo "🍺 Homebrew prefix: $(brew --prefix 2>/dev/null)"

echo ""
echo "💡 To add psql to PATH manually, find the correct path above and run:"
echo "   echo 'export PATH=\"/correct/path/to/bin:\$PATH\"' >> ~/.zshrc"
echo "   source ~/.zshrc"