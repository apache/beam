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

// This cron job runs the Java JMH micro-benchmark suites (on Sundays).
CronJobBuilder.cronJob('beam_Java_Jmh', 'H 0 * * 0', this) {
  description('Runs the Java JMH micro-benchmark suites.')

  // Set common parameters, timeout is set to 12h (benchmarks are expected to run ~8.5h)
  commonJobProperties.setTopLevelMainJobProperties(delegate, 'master', 720, true, 'beam-perf')
  InfluxDBCredentialsHelper.useCredentials(delegate)

  // Gradle goals for this job.
  steps {
    environmentVariables {
      env("INFLUXDB_HOST", InfluxDBCredentialsHelper.InfluxDBHostUrl)
      env("INFLUXDB_DATABASE", InfluxDBCredentialsHelper.InfluxDBDatabaseName)
    }

    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:java:harness:jmh:jmh')
      commonJobProperties.setGradleSwitches(delegate)
    }

    gradle {
      rootBuildScriptDir(commonJobProperties.checkoutDir)
      tasks(':sdks:java:core:jmh:jmh')
      commonJobProperties.setGradleSwitches(delegate)
    }
  }
}
