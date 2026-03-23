#!/bin/bash

# Open AGStream Manager UI in browser
URL="http://localhost:5003/"

echo "Opening AGStream Manager UI at: $URL"
echo ""
echo "If the page doesn't load, make sure the service is running:"
echo "  ./tools/agstream_manager/scripts/manage_agstream.sh status"
echo ""

# Try to open in default browser
if command -v open &> /dev/null; then
    # macOS
    open "$URL"
elif command -v xdg-open &> /dev/null; then
    # Linux
    xdg-open "$URL"
elif command -v start &> /dev/null; then
    # Windows
    start "$URL"
else
    echo "Please open this URL in your browser: $URL"
fi

# Made with Bob
