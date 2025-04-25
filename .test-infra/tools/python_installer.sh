#!/usr/bin/env bash
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
#    See the License for the spefic language governing permissions and
#    limitations under the License.
#
#    Python versions installer via pyenv.
#
set -euo pipefail

# Variable containing the python versions to install
python_versions_arr=("3.9.16" "3.10.10" "3.11.4", "3.12.6")

# Install pyenv dependencies.
pyenv_dep(){
  sudo apt-get update -y
  sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
}

# Install pyenv, delete previous version if needed.
pyenv_install(){
  if [[ -d "$HOME"/.pyenv/ ]]; then
    sudo rm -r "$HOME"/.pyenv
  fi
  if [[ -e "$HOME"/pyenv_installer.sh ]]; then
    sudo rm "$HOME"/pyenv_installer.sh
  fi
  curl https://pyenv.run > "$HOME"/pyenv_installer.sh
  chmod +x "$HOME"/pyenv_installer.sh
  "$HOME"/pyenv_installer.sh
}

# Setting pyenv in User environment, PATH.
pyenv_post_install(){
  if ! < "$HOME"/.bashrc grep -q "# pyenv Config" ; then
    {
      echo '# pyenv Config'
      echo 'export PYENV_ROOT="$HOME/.pyenv"'
      echo 'export PATH="$PYENV_ROOT/bin:$PATH"'
      echo 'if which pyenv > /dev/null; then'
      echo '  eval "$(pyenv init -)"'
      echo '  eval "$(pyenv init --path)"'
      echo '  eval "$(pyenv virtualenv-init -)"'
      echo 'fi'
    } >> "$HOME"/.bashrc
  fi
}

# Install python versions with pyenv
pyenv_versions_install(){
  arr=("$@")
  for version in "${arr[@]}"; do
    "$HOME"/.pyenv/bin/pyenv install "$version"
  done
}

# Setting python versions globally 
python_versions_setglobally(){
  "$HOME"/.pyenv/bin/pyenv global "$@"
}

# Remove pyenv script installer
clean(){
  sudo rm "$HOME"/pyenv_installer.sh
  echo -e "\nRestart your shell so the path changes take effect"
  echo "    'exec $SHELL'" 
}

# Install pyenv environment with python versions
python_installer(){
  pyenv_dep
  pyenv_install
  pyenv_post_install
  pyenv_versions_install "${python_versions_arr[@]}"
  python_versions_setglobally "${python_versions_arr[@]}"
  clean
}

python_installer
