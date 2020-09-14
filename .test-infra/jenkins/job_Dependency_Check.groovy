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

job('beam_Dependency_Check') {
  description('Runs Beam dependency check.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(
      delegate, 'master', 100, true, 'beam', false)

  // Allows triggering this build against pull requests.
  commonJobProperties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Beam Dependency Check',
      'Run Dependency Check',
      false
      )

  // This is a job that runs weekly.
  commonJobProperties.setAutoJob(
      delegate,
      '0 12 * * 1')

  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks('runBeamDependencyCheck')
      commonJobProperties.setGradleSwitches(delegate)
      switches('-Drevision=release')
    }

    shell('cd ' + commonJobProperties.checkoutDir +
        ' && bash .test-infra/jenkins/dependency_check/generate_report.sh')
  }

  wrappers{
    credentialsBinding {
      usernamePassword('BEAM_JIRA_BOT_USERNAME', 'BEAM_JIRA_BOT_PASSWORD', 'beam-jira-bot')
    }
  }

  def date = new Date().format('yyyy-MM-dd')
  publishers {
    extendedEmail {
      triggers {
        always {
          recipientList('dev@beam.apache.org')
          contentType('text/html')
          subject("Beam Dependency Check Report (${date})")
          content('''${FILE, path="src/build/dependencyUpdates/beam-dependency-check-report.html"}''')
        }
      }
    }
    archiveArtifacts {
      pattern('src/build/dependencyUpdates/beam-dependency-check-report.html')
      onlyIfSuccessful()
    }
    wsCleanup {
      excludePattern('src/build/dependencyUpdates/beam-dependency-check-report.html')
    }
  }
}
