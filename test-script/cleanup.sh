#!/bin/bash

HOST="127.0.0.1"
PORT="55000"

if nc -z $HOST $PORT; then
  echo "Server is running on $HOST:$PORT. Skipping cleanup."
  exit 1
fi

cd "$(dirname "$0")/.." || exit 1

echo "Cleaning DB files..."
rm -f sstable-*.db
rm -f MANIFEST
rm -f wal.log

echo "Done."