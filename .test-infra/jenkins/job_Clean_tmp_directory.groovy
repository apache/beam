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

job('beam_Clean_tmp_directory') {
  description('Cleans /tmp directory on the node machine by removing files that were not accessed for long (configurable) time.')

  logRotator {
    daysToKeep(14)
  }

  concurrentBuild()

  parameters {
    labelParam('machine_label') {
      description("Label of the machine to be cleaned. Could be either a specific machine name or `beam` if you want to cleanup all the machines.")
      allNodes('allCases','AllNodeEligibility')
    }
    stringParam {
      name("unaccessed_for")
      defaultValue("10")
      description("Only files that were not accessed for last `unaccessed_for` hours will be deleted. Default value should be right in most cases. Modify it only if know what you're doing :)")
      trim(true)
    }
  }

  steps {
    shell('echo "Current size of /tmp dir is \$(sudo du -sh /tmp)"')
    shell('echo "Deleting files accessed later than \${unaccessed_for} hours ago"')
    shell('sudo find /tmp -type f -amin +\$((60*\${unaccessed_for})) -print -delete')
    shell('echo "Size of /tmp dir after cleanup is \$(sudo du -sh /tmp)"')
  }
}
