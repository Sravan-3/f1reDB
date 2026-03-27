#!/bin/bash

for i in {1..10}; do
(
  while true; do
    key=$(head /dev/urandom | tr -dc a-z0-9 | head -c 5)
    val=$(head /dev/urandom | tr -dc a-z0-9 | head -c 8)
    echo "SET $key $val"
    sleep 0.1
  done
) | nc 127.0.0.1 55000 &
done

wait