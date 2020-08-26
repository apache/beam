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

import CommonJobProperties as common

class Docker {
  private def job
  private String repositoryRoot

  Docker(job, String repositoryRoot) {
    this.job = job
    this.repositoryRoot = repositoryRoot
  }

  /**
   * Builds a Docker image from a gradle task and pushes it to the registry.
   *
   * @param gradleTask - name of a Gradle task
   * @param imageTag - tag of a docker image
   */
  final void publish(String gradleTask, String imageTag = 'latest') {
    job.steps {
      gradle {
        rootBuildScriptDir(common.checkoutDir)
        common.setGradleSwitches(delegate)
        tasks(gradleTask)
        switches("-Pdocker-repository-root=${repositoryRoot}")
        switches("-Pdocker-tag=${imageTag}")
      }
    }
  }

  /**
   * Returns the name of a docker image in the following format: <repositoryRoot>/<imageName>:<imageTag>
   *
   * @param imageName - name of a docker image
   * @param imageTag - tag of a docker image
   */
  final String getFullImageName(String imageName, String imageTag = 'latest') {
    String image = "${repositoryRoot}/${imageName}"
    return "${image}:${imageTag}"
  }
}
