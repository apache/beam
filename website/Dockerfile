###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

# This image contains Ruby and dependencies required to build and test the Beam
# website. It is used by tasks in build.gradle.

FROM ruby:2.5

WORKDIR /ruby
RUN gem install bundler
# Update buildDockerImage's inputs.files if you change this list.
ADD Gemfile Gemfile.lock /ruby/
RUN bundle install --deployment --path $GEM_HOME

# Required for website testing using HTMLProofer.
ENV LC_ALL C.UTF-8

CMD sleep 3600
