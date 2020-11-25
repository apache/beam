#!/bin/bash

# This creates a new virtual volume for the container. This is useful to have
# changes persist across container runs.
VOLUMNE_NAME=beam_docker

# This will be the user created in the container. The default password is
# `password`.
USER=user

# Copy user configuration files to this directory. These will be copied to the
# container.
# Uncomment if you want to copy your config files to the container.
# Make sure to also uncomment the `COPY` commands in `Dockerfile`.
# HOME_DIR=
# cp $HOME_DIR/.bashrc .
# cp $HOME_DIR/.vimrc .
# cp $HOME_DIR/.profile .
# cp $HOME_DIR/.gitconfig .

# Create a persistent volume for the container.
docker volume create --name $VOLUMNE_NAME

# Build the container tagged as `dev:latest`.
docker build -t dev:latest . --build-arg USER=$USER
