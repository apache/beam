# Development Environment Docker Container

## Intro

These files are used to create a docker container that holds a properly
configured development environment for Apache Beam. The intended usage is to
start a shell into the container where the user can then develop with.

## Dependencies

[Docker](https://docs.docker.com/get-docker/)

## How-to build

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
