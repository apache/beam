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
import PostcommitJobBuilder

final jiraTestResultReporterConfigTemplate = '''
  {
    "projectKey": "BEAM",
    "issueType": 1,
    "configs": [
      {
        "clazz": "StringFields",
        "properties": {
          "fieldKey": "summary",
          "value": "${DEFAULT_SUMMARY}"
        }
      },
      {
        "clazz": "StringFields",
        "properties": {
          "fieldKey": "description",
          "value": "${DEFAULT_DESCRIPTION}"
        }
      },
      {
        "clazz": "SelectableArrayFields",
        "properties": {
          "fieldKey": "components",
          "values": [
            {
              "value": "12334203"
            }
          ]
        }
      }
    ],
    "autoRaiseIssue": true,
    "autoResolveIssue": false,
    "autoUnlinkIssue": false
  }
  '''

final jobName = 'beam_PostCommit_Java_GradleBuild'

// This job runs the Java postcommit tests, including the suite of integration
// tests.
PostcommitJobBuilder.postCommitJob(jobName, 'Run Java PostCommit',
  'Java SDK Post Commit Tests', this) {

  description('Runs PostCommit tests on the Java SDK.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 240)

  // def jenkinsHome = scope.JENKINS_HOME
  def fname = "${JENKINS_HOME}/jobs/${jobName}/JiraIssueJobConfigs.json"
  def file = new File(fname)
  new File(file.getParent()).mkdirs()

  file.write jiraTestResultReporterConfigTemplate

  // Publish all test results to Jenkins
  publishers {
    archiveJunit('**/build/test-results/**/*.xml')
  }

  configure { 
    it / 'publishers' / 'hudson.tasks.junit.JUnitResultArchiver' / 'testDataPublishers' / 'org.jenkinsci.plugins.JiraTestResultReporter.JiraTestDataPublisher' {}
  }

  // Gradle goals for this job.
  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':javaPostCommit')
      commonJobProperties.setGradleSwitches(delegate)
      // Specify maven home on Jenkins, needed by Maven archetype integration tests.
      switches('-Pmaven_home=/home/jenkins/tools/maven/apache-maven-3.5.2')
      // BEAM-5035: Parallel builds are very flaky
      switches('--no-parallel')
    }
  }
}
