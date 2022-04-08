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
import InfluxDBCredentialsHelper

job('beam_Metrics_Report') {
  description('Runs Beam metrics report.')

  // Set common parameters.
  commonJobProperties.setTopLevelMainJobProperties(
      delegate, 'master', 100, true, 'beam', false)
  InfluxDBCredentialsHelper.useCredentials(delegate)

  def influxDb = InfluxDBCredentialsHelper.InfluxDBDatabaseName
  def influxHost = InfluxDBCredentialsHelper.InfluxDBHost
  def influxPort = InfluxDBCredentialsHelper.InfluxDBPort

  // Allows triggering this build against pull requests.
  commonJobProperties.enablePhraseTriggeringFromPullRequest(
      delegate,
      'Beam Metrics Report',
      'Run Metrics Report',
      false
      )

  commonJobProperties.setAutoJob(
      delegate,
      '@weekly')

  steps {
    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      commonJobProperties.setGradleSwitches(delegate)
      switches("-PinfluxDb=${influxDb}")
      switches("-PinfluxHost=${influxHost}")
      switches("-PinfluxPort=${influxPort}")
      tasks(':beam-test-jenkins:generateMetricsReport')
    }
  }

  def date = new Date().format('yyyy-MM-dd')
  publishers {
    extendedEmail {
      triggers {
        always {
          recipientList('dev@beam.apache.org')
          contentType('text/html')
          subject("Beam Metrics Report (${date})")
          content('''${FILE, path="src/.test-infra/jenkins/metrics_report/beam-metrics_report.html"}''')
        }
      }
    }
    archiveArtifacts {
      pattern('src/.test-infra/jenkins/metrics_report/beam-metrics_report.html')
      onlyIfSuccessful()
    }
    wsCleanup {
      excludePattern('src/.test-infra/jenkins/metrics_report/beam-metrics_report.html')
    }
  }
}
