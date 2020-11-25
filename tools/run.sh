#!/bin/bash

# This creates a new virtual volume for the container. This is useful to have
# changes persist across container runs.
VOLUMNE_NAME=beam_docker

# This is the user created in the container. The default password is
# `password`.
USER=user

# This runs with the following options:
#  -i run in interactive mode (pipes stdin)
#  -t allocates a pseudo-TTY used to open an interactive shell
#  --rm removes the container after closing
#  -v links the container's /home/$USER/beam directory to the volume
#  dev:latest the container created from the build.sh file
#  /bin/bash starts a shell in the container
docker run -it --rm \
  -v $VOLUMNE_NAME:/home/$USER/beam \
  dev:latest /bin/bash
