FROM ubuntu:groovy
ARG USER

##################################################
# Install python, Java, misc utilities, and tools.
##################################################
RUN apt -y update
RUN apt -y install apt-utils
RUN yes | unminimize
RUN apt -y install python3.8, python3.8-venv, python3.8-distutils, python-is-python3
RUN apt -y install openjdk-8-jdk, python-setuptools, virtualenv, tox
RUN apt -y install vim
RUN apt -y install git
RUN apt -y install sudo
RUN apt -y install man-db
RUN apt -y install time
RUN apt -y install bash-completion


#########################################################################
# Add the $USER to the container with the default password of `password`.
#########################################################################
RUN echo "User is "$USER
RUN useradd -ms /bin/bash $USER
RUN adduser $USER sudo
RUN echo "user:password" | chpasswd
USER $USER
WORKDIR /home/$USER


########################################################
# Install and compile Apache Beam dev files from Github.
########################################################
RUN git clone https://github.com/apache/beam.git
RUN cd beam/sdks/python && virtualenv env
RUN beam/gradlew
RUN beam/gradlew -p beam/ :sdks:python:build
RUN beam/sdks/python/env/bin/pip install -r beam/sdks/python/build-requirements.txt
RUN beam/sdks/python/env/bin/pip install beam/sdks/python[gcp,test,interactive]
RUN cd beam && git remote rename origin upstream


################################
# Copy user customization files.
################################
# Uncomment if you want to copy your config files to the container.
# Make sure to also uncomment the `cp` commands in `build.sh`.
# COPY --chown=$USER .bashrc /home/$USER
# COPY --chown=$USER .profile /home/$USER
# COPY --chown=$USER .gitconfig /home/$USER
# COPY --chown=$USER .vimrc /home/$USER


USER $USER
