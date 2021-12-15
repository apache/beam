#!/bin/bash
nohup /opt/mitmproxy/mitmdump -s /opt/mitmproxy/allow_list_proxy.py -p 8081 &
while [ ! -f /root/.mitmproxy/mitmproxy-ca.pem ] ;
do
      sleep 2
done
openssl x509 -in /root/.mitmproxy/mitmproxy-ca.pem -inform PEM -out /root/.mitmproxy/mitmproxy-ca.crt
cp /root/.mitmproxy/mitmproxy-ca.crt /usr/local/share/ca-certificates/extra/
update-ca-certificates
/opt/playground/backend/server_python_backend
