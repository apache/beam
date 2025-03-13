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

/**
 * Use this class to define jobs that are triggered only using cron.
 */
class CronJobBuilder {
  private def scope
  private def jobDefinition

  CronJobBuilder(scope, jobDefinition = {}) {
    this.scope = scope
    this.jobDefinition = jobDefinition
  }

  /**
   * Set the job details.
   *
   * @param nameBase Job name
   * @param scope Delegate for the job.
   * @param cronPattern Defines when the job should be fired. Default: "every 6th hour".
   * @param jobDefinition Closure for the job.
   */
  static void cronJob(nameBase, cronPattern = 'H H/6 * * *', scope, jobDefinition = {}) {
    CronJobBuilder builder = new CronJobBuilder(scope, jobDefinition)
    builder.defineAutoPostCommitJob(nameBase, cronPattern)
  }

  void defineAutoPostCommitJob(name, cronPattern) {
    def autoBuilds = scope.job(name) {
      commonJobProperties.setAutoJob(delegate, cronPattern, 'builds@beam.apache.org', true)
    }

    autoBuilds.with(jobDefinition)
  }
}
