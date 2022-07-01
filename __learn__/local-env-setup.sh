#!/usr/bin/env bash

darwin_install_pip3_packages() {
  echo "Installing setuptools grpcio-tools"
  pip3 install setuptools grpcio-tools
  echo "Installing mypy-protobuf"
  pip3 install --user mypy-protobuf
}

install_go_packages() {
  echo "Installing goavro"
  go get github.com/linkedin/goavro
  # assume .bashrc is available since we use bash
  grep -qxF "export GOPATH=${PWD}/sdks/go/examples/.gogradle/project_gopath" ~/.bashrc
  gopathExists=$?
  if [ $gopathExists -ne 0 ]; then
    export GOPATH=${PWD}/sdks/go/examples/.gogradle/project_gopath && echo "GOPATH was set for this session to '$GOPATH'. Make sure to add this path to your ~/.bashrc file after this script has run"
  fi
}

kernelname=$(uname -s)

# Running on Linux
if [ "$kernelname" = "Linux" ]; then
  # Beam assumes Debian and specified prerequisites https://beam.apache.org/contribute/
  apt-get update

  echo "Installing pkglist listed dependencies"
  apg-get install -y $(grep -v '^#' dev-support/docker/pkglist | cat)

  type -P python3 > /dev/null 2>&1
  python3Exists=$?
  if [ $python3Exists -eq 0 -a $pip3Exists -eq 0 ]; then
    echo "Installing grpcio-tools mypy-protobuf"
    pip3 install grpcio-tools mypy-protobuf
  else
    echo "Python3 and pip3 are required but failed to install. Install them manually and rerun the script"
    exit
  fi

  type -P go > /dev/null 2>&1
  goExists=$?
  if [ $goExists -eq 0 ]; then
    install_go_packages
  else
    echo "Go is required, please download manually and install, then re-run the script"
    exit
  fi

# Running on Mac
elif [ "$kernelname" = "Darwin" ]; then
  # check for homebrew, install if not available
  type -b brew > /dev/null 2>&1
  brewExists=$?
  if [ $brewExists -ne 0 ]; then
    echo "Installing homebrew"
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  fi

  # update homebrew recipes
  echo "Updating brew"
  brew update

  # assume brew is available
  if brew ls --versions openjdk@8 > /dev/null; then
    echo "openjdk@8 already installed, skipping..."
  else
    echo "installing openjdk@8"
  fi
  for ver in 3.7 3.8 3.9; do
    if brew ls --versions python@$ver > /dev/null; then
      echo "python@$ver already installed, skipping"
      brew info python@$ver
    else
      echo "installing python@$ver"
      brew install python@$ver
    fi
    if [ ! $(type -P python$ver) > /dev/null 2>&1 ]; then
      # for certain python packages brew doesn't add symlinks
      # TODO: consider using pyenv to manage multiple installations of python
      ln -s /usr/local/opt/python@$ver/bin/python3 /usr/local/bin/python$ver
    fi
  done