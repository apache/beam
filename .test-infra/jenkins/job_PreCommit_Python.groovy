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

import PrecommitJobBuilder

PrecommitJobBuilder builder = new PrecommitJobBuilder(
    scope: this,
    nameBase: 'Python',
    gradleTask: ':pythonPreCommit',
    gradleSwitches: [
      '-Pposargs=\'apache_beam/*.py apache_beam/coders apache_beam/internal apache_beam/ml apache_beam/options apache_beam/portability apache_beam/testing apache_beam/tools apache_beam/typehints apache_beam/utils\'' // All other tests are covered by different jobs.
    ],
    timeoutMins: 180,
    triggerPathPatterns: [
      '^model/.*$',
      '^sdks/python/.*$',
      '^release/.*$',
    ]
    )
builder.build {
  // Publish all test results to Jenkins.
  publishers {
    archiveJunit('**/pytest*.xml')
  }
}
