#!/usr/bin/env bash
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file
#    distributed with this work for additional information
#    regarding copyright ownership.  The ASF licenses this file
#    to you under the Apache License, Version 2.0 (the
#    "License"); you may not use this file except in compliance
#    with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

set -euo pipefail

if [[ $# -ne 3 || ! $3 =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 HOST PORT TIMEOUT_SECONDS" >&2
  exit 2
fi

host=$1
port=$2
timeout_seconds=$3

# Keep the script unexpanded until the child Bash receives its arguments.
# shellcheck disable=SC2016
if timeout --signal=TERM --kill-after=0.1s "${timeout_seconds}s" bash -c '
  host=$1
  port=$2
  deadline=$((SECONDS + $3))

  while (( SECONDS < deadline )); do
    remaining=$((deadline - SECONDS))
    connect_timeout=5
    if (( remaining < connect_timeout )); then
      connect_timeout=$remaining
    fi

    if timeout --signal=KILL "${connect_timeout}s" bash -c \
      "exec 3<>\"/dev/tcp/\$1/\$2\"" _ "$host" "$port" 2>/dev/null; then
      printf "SingleStore endpoint %s:%s is accepting TCP connections\n" "$host" "$port"
      exit 0
    fi

    remaining=$((deadline - SECONDS))
    if (( remaining > 0 )); then
      sleep "$(( remaining < 2 ? remaining : 2 ))"
    fi
  done

  exit 124
' _ "$host" "$port" "$timeout_seconds"; then
  exit 0
else
  status=$?
  if (( status == 124 || status == 137 || status == 143 )); then
    echo "::error::SingleStore endpoint $host:$port did not accept a TCP connection within ${timeout_seconds}s"
  fi
  exit "$status"
fi
