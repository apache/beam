#!/bin/bash

# Install RVM.
gpg --keyserver hkp://keys.gnupg.net --recv-keys \
    409B6B1796C275462A1703113804BB82D39DC0E3 \
    7D2BAF1CF37B13E2069D6956105BD0E739499BDB

curl -sSL https://get.rvm.io | bash
source /home/jenkins/.rvm/scripts/rvm

# Install Ruby.
RUBY_VERSION_NUM=2.3.0
rvm install ruby $RUBY_VERSION_NUM --autolibs=read-only

# Install Bundler gem
PATH=~/.gem/ruby/$RUBY_VERSION_NUM/bin:$PATH
GEM_PATH=~/.gem/ruby/$RUBY_VERSION_NUM/:$GEM_PATH
gem install bundler --user-install

# Install all needed gems.
bundle install --path ~/.gem/
