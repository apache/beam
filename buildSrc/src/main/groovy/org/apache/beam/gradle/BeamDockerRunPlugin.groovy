/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.gradle

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.Exec

/**
 * A gradle plug-in handling 'docker run' command. Originally replicated from
 * <a href="https://github.com/palantir/gradle-docker">com.palantir.docker-run</a> plugin.
 */
class BeamDockerRunPlugin implements Plugin<Project> {

  /** A class defining the configurations of dockerRun task. */
  static class DockerRunExtension {
    String name
    String image
    Set<String> ports = [] as Set
    Map<String,String> env = [:]
    List<String> arguments = []
    Map<String,String> volumes = [:]
    boolean daemonize = true
    boolean clean = false

    public String getName() {
      return name
    }

    public void setName(String name) {
      this.name = name
    }
  }

  @Override
  void apply(Project project) {
    DockerRunExtension ext = project.extensions.create('dockerRun', DockerRunExtension)

    Exec dockerRunStatus = project.tasks.create('dockerRunStatus', Exec, {
      group = 'Docker Run'
      description = 'Checks the run status of the container'
    })

    Exec dockerRun = project.tasks.create('dockerRun', Exec, {
      group = 'Docker Run'
      description = 'Runs the specified container with port mappings'
    })

    Exec dockerStop = project.tasks.create('dockerStop', Exec, {
      group = 'Docker Run'
      description = 'Stops the named container if it is running'
      ignoreExitValue = true
    })

    Exec dockerRemoveContainer = project.tasks.create('dockerRemoveContainer', Exec, {
      group = 'Docker Run'
      description = 'Removes the persistent container associated with the Docker Run tasks'
      ignoreExitValue = true
    })

    project.afterEvaluate {
      /** Inspect status of docker. */
      dockerRunStatus.with {
        standardOutput = new ByteArrayOutputStream()
        commandLine 'docker', 'inspect', '--format={{.State.Running}}', ext.name
        doLast {
          if (standardOutput.toString().trim() != 'true') {
            println "Docker container '${ext.name}' is STOPPED."
            return 1
          } else {
            println "Docker container '${ext.name}' is RUNNING."
          }
        }
      }

      /**
       * Run a docker container. See {@link DockerRunExtension} for supported
       * arguments.
       *
       * Replication of dockerRun task of com.palantir.docker-run plugin.
       */
      dockerRun.with {
        List<String> args = new ArrayList()
        args.addAll(['docker', 'run'])

        if (ext.daemonize) {
          args.add('-d')
        }
        if (ext.clean) {
          args.add('--rm')
        } else {
          finalizedBy dockerRunStatus
        }
        for (String port : ext.ports) {
          args.add('-p')
          args.add(port)
        }
        for (Map.Entry<Object,String> volume : ext.volumes.entrySet()) {
          File localFile = project.file(volume.key)

          if (!localFile.exists()) {
            logger.error("ERROR: Local folder ${localFile} doesn't exist. Mounted volume will not be visible to container")
            throw new IllegalStateException("Local folder ${localFile} doesn't exist.")
          }
          args.add('-v')
          args.add("${localFile.absolutePath}:${volume.value}")
        }
        args.addAll(ext.env.collect{ k, v -> ['-e', "${k}=${v}"]}.flatten())
        args.add('--name')
        args.add(ext.name)
        if (!ext.arguments.isEmpty()) {
          args.addAll(ext.arguments)
        }
        args.add(ext.image)

        commandLine args
      }

      dockerStop.with {
        commandLine 'docker', 'stop', ext.name
      }

      dockerRemoveContainer.with {
        commandLine 'docker', 'rm', ext.name
      }
    }
  }
}
