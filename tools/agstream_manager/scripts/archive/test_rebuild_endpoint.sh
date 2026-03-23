#!/bin/bash
# Test script to verify the rebuild UDFs endpoint

echo "Testing /api/flink/rebuild-udfs endpoint..."
echo ""

# Test the endpoint
curl -X POST http://localhost:5003/api/flink/rebuild-udfs \
  -H "Content-Type: application/json" \
  -w "\n\nHTTP Status: %{http_code}\n" \
  2>/dev/null

echo ""
echo "If you see a 200 status and success:true, the endpoint is working!"
echo "If you see connection refused, make sure the AGStream Manager service is running."

# Made with Bob
