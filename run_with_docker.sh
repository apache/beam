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
#

# This script will run Jekyll inside Docker to build the site.
#
# Use "./run_with_docker.sh server" to build the site at localhost:4000.
# Use "./run_with_docker.sh test" to test for any broken links.
#
# The script assumes you have docker installed on the machine.

_runner() {
  docker run --rm -it -v "$PWD":/srv/jekyll -p 4000:4000 \
    --env="BUNDLE_CACHE=true" \
    --env="BUNDLE_PATH=/srv/jekyll/vendor/bundle" \
    jekyll/jekyll \
    bash -c "bundle config build.nokogiri --use-system-libraries && bundle install && $@"
}

# If no argument is passed then just print the help
if [[ $# -ne 1 ]] ; then
  echo "Method to run was not passed, should be server/test";
  exit 0;
fi

case "$1" in
  server)
    _runner "bundle exec jekyll server --force_polling --watch -H 0.0.0.0 -P 4000 --incremental";
    ;;
  test)
    _runner "bundle exec rake test";
    ;;
  *)
    echo "Unknown Argument $1 passed to the script";
    ;;
esac;
