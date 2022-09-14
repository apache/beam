FROM python:3.8-slim

# optional: install extra dependencies

# install python packages
# (the requirements file is currently empty
# because this is a stub ingestion example)
COPY requirements.txt /
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# copy src files and set working directory
COPY src /src
WORKDIR /src
