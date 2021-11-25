FROM mitmproxy/mitmproxy:7.0.4

COPY allow_list_proxy.py /
COPY allow_list.py /
EXPOSE 8080
ENTRYPOINT ["mitmdump", "-s", "allow_list_proxy.py"]
