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

gosu mitmproxy mitmdump
