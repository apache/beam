#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

nohup /opt/mitmproxy/mitmdump -s /opt/mitmproxy/allow_list_proxy.py -p 8081 &
while [ ! -f /home/appuser/.mitmproxy/mitmproxy-ca.pem ] ;
do
      sleep 2
done
openssl x509 -in /home/appuser/.mitmproxy/mitmproxy-ca.pem -inform PEM -out /home/appuser/.mitmproxy/mitmproxy-ca.crt
cp /home/appuser/.mitmproxy/mitmproxy-ca.crt /usr/local/share/ca-certificates/extra/
update-ca-certificates

/opt/playground/backend/server_go_backend
