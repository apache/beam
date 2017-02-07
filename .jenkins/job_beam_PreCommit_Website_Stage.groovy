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

import common_job_properties

// Defines a job.
job('beam_PreCommit_Website_Stage') {
  description('Stages the pull requests proposed for the Apache Beam ' +
              'website to a temporary location to ease reviews.')

  // Set common parameters.
  common_job_properties.setTopLevelWebsiteJobProperties(delegate)

  // Set pull request build trigger.
  common_job_properties.setPreCommit(
      delegate,
      'Automatic staging of pull requests',
      '\nJenkins built the site at commit id ${ghprbActualCommit} with ' +
      'Jekyll and staged it [here](http://apache-beam-website-pull-' +
      'requests.storage.googleapis.com/${ghprbPullId}/index.html). ' +
      'Happy reviewing.\n\nNote that any previous site has been deleted. ' +
      'This staged site will be automatically deleted after its TTL ' +
      'expires. Push any commit to the pull request branch or re-trigger ' +
      'the build to get it staged again.')

  steps {
    // Run the following shell script as a build step.
    shell '''
        # Install RVM.
        gpg --keyserver hkp://keys.gnupg.net --recv-keys \\
            409B6B1796C275462A1703113804BB82D39DC0E3
        \\curl -sSL https://get.rvm.io | bash
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

        # Remove current site if it exists.
        GCS_PATH="gs://apache-beam-website-pull-requests/${ghprbPullId}/"
        gsutil -m rm -r -f ${GCS_PATH} || true

        # Build the new site with the baseurl specified.
        rm -fr ./content/
        bundle exec jekyll build --baseurl=/${ghprbPullId}

        # Install BeautifulSoup HTML Parser for python.
        pip install --user beautifulsoup4

        # Fix links on staged website.
        python .jenkins/append_index_html_to_internal_links.py

        # Upload the new site.
        gsutil -m cp -R ./content/* ${GCS_PATH}
    '''.stripIndent().trim()
  }
}
