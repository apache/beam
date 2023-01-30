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

openssl x509 -in /home/appuser/.mitmproxy/mitmproxy-ca.pem -inform PEM -out /usr/local/share/ca-certificates/extra/mitmproxy-ca.crt
keytool -importcert -trustcacerts -storepass changeit -alias mitmproxy -file /home/appuser/.mitmproxy/mitmproxy-ca-cert.pem -noprompt -keystore $JAVA_HOME/jre/lib/security/cacerts
update-ca-certificates

mkdir -p ~/jar-update
pushd ~/jar-update|| exit
jar xf /opt/google-api-client.jar com/google/api/client/googleapis/google.p12
keytool -importcert -trustcacerts -storepass notasecret -alias mitmproxy -file /home/appuser/.mitmproxy/mitmproxy-ca-cert.pem -noprompt -keystore com/google/api/client/googleapis/google.p12
zip -u /opt/google-api-client.jar com/google/api/client/googleapis/google.p12
popd || exit
rm -r ~/jar-update

su appuser -c /opt/playground/backend/server_scio_backend
