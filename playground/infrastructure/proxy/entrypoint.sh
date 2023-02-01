#!/bin/bash

set -o errexit
set -o pipefail

MITMPROXY_PATH="/home/mitmproxy/.mitmproxy"

if [ -f "$MITMPROXY_PATH/mitmproxy-ca.pem" ]; then
  f="$MITMPROXY_PATH/mitmproxy-ca.pem"
else
  f="$MITMPROXY_PATH"
fi
usermod -o \
    -u $(stat -c "%u" "$f") \
    -g $(stat -c "%g" "$f") \
    mitmproxy

if [[ "$1" = "reverseproxy" ]]; then
  shift
  while [[ "$1" != "" ]]; do
    option=$1
    IFS=':' read -ra options <<< "$option"
    external=${options[0]}
    host=${options[1]}
    port=${options[2]}
    gosu mitmproxy mitmdump -p "$external" --mode reverse:http://"$host":$port &
    shift
  done
fi

gosu mitmproxy mitmdump -s /home/mitmproxy/allow_list_proxy.py -p 8080 --ignore-hosts "storage.googleapis.com"
