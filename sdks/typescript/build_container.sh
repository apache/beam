#!/bin/sh

# TODO: Do this via a gradle rather than manually.
env GOOS=linux GOARCH=amd64 go build boot.go
docker build . -t apache/beam_typescript_sdk:latest -t gcr.io/apache-beam-testing/beam_typescript_sdk:dev
