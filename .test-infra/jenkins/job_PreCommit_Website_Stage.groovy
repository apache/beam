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

import CommonJobProperties as commonJobProperties
import WebsiteShared as websiteShared

// TODO(BEAM-4505): This job is for the apache/beam-site repository and
// should be removed once website sources are migrated to apache/beam.

// Defines a job.
job('beam_PreCommit_Website_Stage') {
  description('Stages the pull requests proposed for the Apache Beam ' +
              'website to a temporary location to ease reviews.')

  // Set common parameters.
  commonJobProperties.setTopLevelWebsiteJobProperties(delegate)

  // Set pull request build trigger.
  commonJobProperties.setPreCommit(
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
    shell """
        ${websiteShared.install_ruby_and_gems_bash}

        # Remove current site if it exists.
        GCS_PATH="gs://apache-beam-website-pull-requests/\${ghprbPullId}/"
        gsutil -m rm -r -f \${GCS_PATH} || true

        # Build the new site with the baseurl specified.
        rm -fr ./content/
        bundle exec jekyll build --baseurl=/\${ghprbPullId}

        # Install BeautifulSoup HTML Parser for python.
        pip install --user beautifulsoup4

        # Fix links on staged website.
        python .jenkins/append_index_html_to_internal_links.py

        # Upload the new site.
        gsutil -m cp -R ./content/* \${GCS_PATH}
    """.stripIndent().trim()
  }
}
