# [START component_dockerfile]
FROM python:3.8-slim

# (Optional) install extra dependencies

# install pypi dependencies
COPY requirements.txt /
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# copy src files and set working directory
COPY src /src
WORKDIR /src
# [END component_dockerfile]
