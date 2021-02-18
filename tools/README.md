# Development Environment Docker Container

## Intro

These files are used to create a docker container that holds a properly
configured development environment for Apache Beam. The intended usage is to
start a shell into the container where the user can then develop with.

## Dependencies

[Docker](https://docs.docker.com/get-docker/)

## How it works
This creates a new Docker volume parameterized by `VOLUME_NAME` in the build.sh
and in run.sh.  This is to create a persistent volume to use while doing develpoment work. It's also good in case the docker container crashes for any reason so that you will still have your work. Feel free to change this to a volume name of your choice.

The build script then creates a Docker container with the dependencies installed
for Python and Java development.

## How-to build

If you want to add your bashrc/profilec/etc. configurations then open up
build.sh in a text editor and uncomment out the lines pertaining to HOME_DIR.
Then set the HOME_DIR variable to the absolute path to your home directory.

If you want to add VIM customizations to the container go the the VIM
customization section in the `Dockerfile` and uncomment out that section.
```
git clone https://github.com/apache/beam.git <beam root>

cd <beam root>/tools
sudo ./build.sh
```

## How-to run

```
cd <beam root>/tools
sudo ./run.sh
```

## First time run instructions

When the container is built for the first time, the git repo on the volume won't
be linked to your github account. To do this, follow the instructions at
https://docs.github.com/en/github/using-git/adding-a-remote. Note that the
Apache Beam repo was already added as `upstream`.
