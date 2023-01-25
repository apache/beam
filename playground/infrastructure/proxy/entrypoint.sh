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
  gosu mitmproxy mitmdump -p 8082 --mode reverse:http://router:8082 &
  gosu mitmproxy mitmdump -p 8084 --mode reverse:http://go_runner:8084 &
  gosu mitmproxy mitmdump -p 8086 --mode reverse:http://java_runner:8086 &
  gosu mitmproxy mitmdump -p 8088 --mode reverse:http://python_runner:8088 &
  gosu mitmproxy mitmdump -p 8090 --mode reverse:http://scio_runner:8090 &
  gosu mitmproxy mitmdump -p 1234 --mode reverse:http://frontend:8080 &
fi

gosu mitmproxy mitmdump -s /home/mitmproxy/allow_list_proxy.py -p 8080
