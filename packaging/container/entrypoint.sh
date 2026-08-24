#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
  set -- --listen 0.0.0.0:8080
fi

if [ -n "${K10S_ACCESS_TOKEN_FILE:-}" ]; then
  set -- --token-file "$K10S_ACCESS_TOKEN_FILE" "$@"
fi

exec /usr/local/bin/k10s-server "$@"
