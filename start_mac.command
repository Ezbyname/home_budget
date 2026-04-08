#!/bin/bash
# ============================================
#  Family Budget Tracker — Mac Launcher
#  Double-click this file to start the app
# ============================================

cd "$(dirname "$0")"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo "========================================="
echo "   Family Budget Tracker"
echo "========================================="
echo ""

# Check for Python 3
if command -v python3 &> /dev/null; then
    PY=python3
elif command -v python &> /dev/null; then
    PY=python
else
    echo -e "${RED}Python 3 is not installed.${NC}"
    echo ""
    echo "Install it from: https://www.python.org/downloads/"
    echo "Or run:  brew install python3"
    echo ""
    read -p "Press Enter to close..."
    exit 1
fi

echo -e "${GREEN}Using:${NC} $($PY --version)"

# Install dependencies if needed
if ! $PY -c "import flask" 2>/dev/null; then
    echo -e "${YELLOW}Installing dependencies...${NC}"
    $PY -m pip install --user -q flask==3.1.0 xlrd==2.0.2 openpyxl==3.1.5
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to install dependencies.${NC}"
        echo "Try running:  $PY -m pip install flask xlrd openpyxl"
        read -p "Press Enter to close..."
        exit 1
    fi
    echo -e "${GREEN}Dependencies installed.${NC}"
fi

echo ""
echo -e "${GREEN}Starting the app...${NC}"
echo "The app will open in your browser at: http://127.0.0.1:5000"
echo "To stop: close this window or press Ctrl+C"
echo ""

# Open browser after a short delay
(sleep 2 && open http://127.0.0.1:5000) &

# Start the app
$PY app.py
