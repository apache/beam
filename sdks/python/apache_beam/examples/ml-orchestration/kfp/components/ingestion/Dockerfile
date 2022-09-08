FROM python:3.8-slim

# optional install extra dependencies
# TODO

# install pypi dependencies
COPY requirements.txt /
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# copy src files and set working directory
COPY src /src
WORKDIR /src