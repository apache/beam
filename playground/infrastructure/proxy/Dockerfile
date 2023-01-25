FROM mitmproxy/mitmproxy:9.0.1

COPY allow_list_proxy.py /home/mitmproxy/
COPY allow_list.py /home/mitmproxy/
COPY entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
