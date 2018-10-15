/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO(BEAM-4505): Remove this file once apache/beam-site jobs stop using it.

@groovy.transform.Field static final String install_ruby_and_gems_bash = '''
        maxKeyFetchAttempts=5
        currentAttempt=0
        retryDelaySeconds=15

        while [ $currentAttempt -lt $maxKeyFetchAttempts ]
        do
          echo "Attempt #$currentAttempt"
          gpg --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 7D2BAF1CF37B13E2069D6956105BD0E739499BDB && break
          echo "Attempt failed. Sleeping $retryDelaySeconds seconds, then retrying"
          n=$((n+1))
          sleep $retryDelaySeconds
        done

        if [ $currentAttempt -ge $maxKeyFetchAttempts ]
        then
          echo "ERROR: Failed to fetch gpg keys."
          exit 1
        fi

        \\curl -sSL https://get.rvm.io | bash
        source /home/jenkins/.rvm/scripts/rvm

        # Install Ruby.
        RUBY_VERSION_NUM=2.3.0
        rvm install ruby $RUBY_VERSION_NUM --autolibs=read-only

        # Install Bundler gem
        PATH=~/.gem/ruby/$RUBY_VERSION_NUM/bin:$PATH
        GEM_PATH=~/.gem/ruby/$RUBY_VERSION_NUM/:$GEM_PATH
        gem install bundler --user-install

        # Enter the git clone for remaining commands
        cd src

        # Install all needed gems.
        bundle install --path ~/.gem/

        '''
