#!/bin/bash
MONGO_KEYFILE="/data/mongo-keyfile"

if [ ! -f "$MONGO_KEYFILE" ]; then
  echo "Generating keyfile..."
  openssl rand -base64 756 > "$MONGO_KEYFILE"
  chmod 400 "$MONGO_KEYFILE"
  chown mongodb:mongodb "$MONGO_KEYFILE"
fi
