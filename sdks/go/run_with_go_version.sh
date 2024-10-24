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
# It requires an existing Go installation greater than 1.16 on the system, which
# will be used to download specific versions of Go.
#
# The Go installation will use the local host platform, while the actual
# go command will use the set GOOS and GOARCH env variables.
#
# Accepts the following optional flags, after which all following parameters
# will be provided to the go version tool.
#    --version -> A string for a fully qualified go version, eg go1.16.5 or go1.18beta1
#        The list of available versions are at https://go.dev/dl/
#    --gocmd -> a specific path to a Go command to execute. If present, ignores --version flag
#        and avoids doing the download check step.



set -e

# The specific Go version used by default for Beam infrastructure.
#
# This variable is also used as the execution command downscript.
# The list of downloadable versions are at https://go.dev/dl/
GOVERS=go1.23.2

if ! command -v go &> /dev/null
then
    echo "go could not be found. This script requires a go installation > 1.16 to bootstrap using specific go versions."
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
    --gocmd)
        GOCMD="$2"
        shift # past argument
        shift # past value
        ;;
    *)  # unknown options are go tool args.
        break
        ;;
esac
done

GOPATH=`go env GOPATH`
GOBIN=$GOPATH/bin
GOHOSTOS=`go env GOHOSTOS`
GOHOSTARCH=`go env GOHOSTARCH`


# Check if we've already prepared the Go command. If so, then we don't need to
# do the download and versioning check.
if [ -z "$GOCMD" ] ; then
    # Outputing the system Go version for debugging purposes.
    echo "System Go installation: `which go` is `go version`; Preparing to use $GOBIN/$GOVERS"
    # Ensure it's installed in the GOBIN directory, using the local host platform.
    GOOS=$GOHOSTOS GOARCH=$GOHOSTARCH GOBIN=$GOBIN go install golang.org/dl/$GOVERS@latest

    LOCKFILE=$GOBIN/$GOVERS.lock
    # The download command isn't concurrency safe so we get an exclusive lock, without wait.
    # If we're first, we ensure the command is downloaded, releasing the lock afterwards.
    # This operation is cached on system and won't be re-downloaded at least.
    flock --exclusive --nonblock --conflict-exit-code 0 $LOCKFILE $GOBIN/$GOVERS download

    # Execute the script with the remaining arguments.
    # We get a shared lock for the ordinary go command execution.
    echo $GOBIN/$GOVERS $@
    flock --shared --timeout=10 $LOCKFILE $GOBIN/$GOVERS $@
else
    # Minor TODO: Figure out if we can pull out the GOCMD env variable after goPrepare
    # completion, and avoid this brittle GOBIN substitution.
    GOCMD=${GOCMD/GOBIN/$GOBIN}

    echo $GOCMD $@
    CGO_ENABLED=0 $GOCMD $@
fi
