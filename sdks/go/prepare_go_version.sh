#!/bin/bash
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

# This script sets the go version used by all Beam SDK scripts.
# It requires an existing Go installation 1.16 or greater on the system, which
# will be used to download specific versions of Go.
#
# Accepts the following require flag:
#    --version -> A string for a fully qualified go version, eg go1.16.5 or go1.18beta1
#        The list of available versions are at https://go.dev/dl/ 


set -e

# The specific Go version used by default for Beam infrastructure.
#
# This variable is also used as the execution command downscript.
# The list of downloadable versions are at https://go.dev/dl/ 
GOVERS="invalid"

if ! command -v go &> /dev/null
then
    echo "go could not be found. This script requires a go installation > 1.16 to bootstrap using specific go versions. See http://go.dev/doc/install for options."
    exit 1
fi

# Versions of Go > 1.16 can download arbitrary versions of go.
# We take advantage of this to avoid changing the user's
# installation of go ourselves, and allow for hermetic reproducible
# builds when relying on Gradle, like Jenkins does.
MINGOVERSION="go1.16.0"

# Compare the go version by sorting only by the version string, getting the
# oldest version, and checking if it contains "min". When it doesn't, it's
# the go print out, and it means the system version is later than the minimum.
if (echo "min version $MINGOVERSION os/arch"; go version) | sort -Vk3 -s |tail -1 | grep -q min;
then 
  # Outputing the system Go version for debugging purposes.
  echo "System Go installation at `which go` is `go version`, is older than the minimum required for hermetic, reproducible Beam builds. Want $MINGOVERSION. See http://go.dev/doc/install for installation instructions."; 
  exit 1
fi

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --version)
        GOVERS="$2"
        shift # past argument
        shift # past value
        ;;
    *)  # unknown args
        echo "prepare_go_version requires the --version flag. See https://go.dev/dl/ for available versions."
        exit 1
        ;;
esac
done

GOPATH=`go env GOPATH`
GOBIN=$GOPATH/bin
GOHOSTOS=`go env GOHOSTOS`
GOHOSTARCH=`go env GOHOSTARCH`

echo "System Go installation: `which go` is `go version`; Preparing to use $GOBIN/$GOVERS"

# Ensure it's installed in the GOBIN directory, using the local host platform.
GOOS=$GOHOSTOS GOARCH=$GOHOSTARCH GOBIN=$GOBIN go install golang.org/dl/$GOVERS@latest

# The download command isn't concurrency safe so prepare should be done at most once
# per gogradle chain.
$GOBIN/$GOVERS download

export GOCMD=$GOBIN/$GOVERS
echo "GOCMD=$GOCMD"
