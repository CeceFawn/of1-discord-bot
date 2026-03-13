#!/bin/bash
set -e
cd "$(dirname "$0")"
source .env 2>/dev/null || true
exec python3 website.py
