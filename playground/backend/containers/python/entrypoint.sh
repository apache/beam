#!/bin/bash
nohup /opt/mitmproxy/mitmdump -s /opt/mitmproxy/allow_list_proxy.py -p 8081 &
while [ ! -f /root/.mitmproxy/mitmproxy-ca.pem ] ;
do
      sleep 2
done
openssl x509 -in /root/.mitmproxy/mitmproxy-ca.pem -inform PEM -out /root/.mitmproxy/mitmproxy-ca.crt
cp /root/.mitmproxy/mitmproxy-ca.crt /usr/local/share/ca-certificates/extra/
update-ca-certificates
cat /root/.mitmproxy/mitmproxy-ca.pem >> /usr/local/lib/python3.7/site-packages/certifi/cacert.pem
/opt/playground/backend/server_python_backend
