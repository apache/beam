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

import java.util.regex.Pattern
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.logging.LogLevel
import org.gradle.api.logging.Logger
import org.gradle.api.logging.Logging
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec

/**
 * A gradle plug-in interacting with docker. Originally replicated from
 * <a href="https://github.com/palantir/gradle-docker">com.palantir.docker</a> plugin.
 */
class BeamDockerPlugin implements Plugin<Project> {
  private static final Logger logger = Logging.getLogger(BeamDockerPlugin.class)
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile('^[a-z0-9.-]*$')

  static class DockerExtension {
    Project project

    private static final String DEFAULT_DOCKERFILE_PATH = 'Dockerfile'
    String name = null
    File dockerfile = null
    String dockerComposeTemplate = 'docker-compose.yml.template'
    String dockerComposeFile = 'docker-compose.yml'
    Set<Task> dependencies = [] as Set
    Set<String> tags = [] as Set
    Map<String, String> namedTags = [:]
    Map<String, String> labels = [:]
    Map<String, String> buildArgs = [:]
    boolean pull = false
    boolean noCache = false
    String network = null
    boolean buildx = false
    Set<String> platform = [] as Set
    boolean load = false
    boolean push = false
    String builder = null

    File resolvedDockerfile = null
    File resolvedDockerComposeTemplate = null
    File resolvedDockerComposeFile = null

    // The CopySpec defining the Docker Build Context files
    final CopySpec copySpec

    DockerExtension(Project project) {
      this.project = project
      this.copySpec = project.copySpec()
    }

    void resolvePathsAndValidate() {
      if (dockerfile != null) {
        resolvedDockerfile = dockerfile
      } else {
        resolvedDockerfile = project.file(DEFAULT_DOCKERFILE_PATH)
      }
      resolvedDockerComposeFile = project.file(dockerComposeFile)
      resolvedDockerComposeTemplate = project.file(dockerComposeTemplate)
    }

    void dependsOn(Task... args) {
      this.dependencies = args as Set
    }

    Set<Task> getDependencies() {
      return dependencies
    }

    void files(Object... files) {
      copySpec.from(files)
    }

    void tags(String... args) {
      this.tags = args as Set
    }

    Set<String> getTags() {
      return this.tags + project.getVersion().toString()
    }

    Set<String> getPlatform() {
      return platform
    }

    void platform(String... args) {
      this.platform = args as Set
    }
  }

  @Override
  void apply(Project project) {
    DockerExtension ext = project.extensions.create('docker', DockerExtension, project)

    Delete clean = project.tasks.create('dockerClean', Delete, {
      group = 'Docker'
      description = 'Cleans Docker build directory.'
    })

    Copy prepare = project.tasks.create('dockerPrepare', Copy, {
      group = 'Docker'
      description = 'Prepares Docker build directory.'
      dependsOn clean
    })

    Exec exec = project.tasks.create('docker', Exec, {
      group = 'Docker'
      description = 'Builds Docker image.'
      dependsOn prepare
      environment 'DOCKER_BUILDKIT', '1'
    })

    Task tag = project.tasks.create('dockerTag', {
      group = 'Docker'
      description = 'Applies all tags to the Docker image.'
      dependsOn exec
    })

    Task pushAllTags = project.tasks.create('dockerTagsPush', {
      group = 'Docker'
      description = 'Pushes all tagged Docker images to configured Docker Hub.'
    })

    project.tasks.create('dockerPush', {
      group = 'Docker'
      description = 'Pushes named Docker image to configured Docker Hub.'
      dependsOn pushAllTags
    })

    project.afterEvaluate {
      ext.resolvePathsAndValidate()
      String dockerDir = "${project.buildDir}/docker"
      clean.delete dockerDir

      prepare.with {
        with ext.copySpec
        from(ext.resolvedDockerfile) {
          rename { fileName ->
            fileName.replace(ext.resolvedDockerfile.getName(), 'Dockerfile')
          }
        }
        into dockerDir
      }

      exec.with {
        workingDir dockerDir
        commandLine buildCommandLine(ext)
        dependsOn ext.getDependencies()
        logging.captureStandardOutput LogLevel.INFO
        logging.captureStandardError LogLevel.ERROR
      }

      Map<String, Object> tags = ext.namedTags.collectEntries { taskName, tagName ->
        [
          generateTagTaskName(taskName),
          [
            tagName: tagName,
            tagTask: {
              -> tagName }
          ]
        ]
      }

      if (!ext.tags.isEmpty()) {
        ext.tags.each { unresolvedTagName ->
          String taskName = generateTagTaskName(unresolvedTagName)

          if (tags.containsKey(taskName)) {
            throw new IllegalArgumentException("Task name '${taskName}' is existed.")
          }

          tags[taskName] = [
            tagName: unresolvedTagName,
            tagTask: {
              -> computeName(ext.name, unresolvedTagName) }
          ]
        }
      }

      tags.each { taskName, tagConfig ->
        Exec tagSubTask = project.tasks.create('dockerTag' + taskName, Exec, {
          group = 'Docker'
          description = "Tags Docker image with tag '${tagConfig.tagName}'"
          workingDir dockerDir
          commandLine 'docker', 'tag', "${-> ext.name}", "${-> tagConfig.tagTask()}"
          dependsOn exec
        })
        tag.dependsOn tagSubTask

        Exec pushSubTask = project.tasks.create('dockerPush' + taskName, Exec, {
          group = 'Docker'
          description = "Pushes the Docker image with tag '${tagConfig.tagName}' to configured Docker Hub"
          workingDir dockerDir
          commandLine 'docker', 'push', "${-> tagConfig.tagTask()}"
          dependsOn tagSubTask
        })
        pushAllTags.dependsOn pushSubTask
      }
    }
  }

  private List<String> buildCommandLine(DockerExtension ext) {
    List<String> buildCommandLine = ['docker']
    if (ext.buildx) {
      buildCommandLine.addAll(['buildx', 'build'])
      if (!ext.platform.isEmpty()) {
        buildCommandLine.addAll('--platform', String.join(',', ext.platform))
      }
      if (ext.load) {
        buildCommandLine.add '--load'
      }
      if (ext.push) {
        buildCommandLine.add '--push'
        if (ext.load) {
          throw new Exception("cannot combine 'push' and 'load' options")
        }
      }
      if (ext.builder != null) {
        buildCommandLine.addAll('--builder', ext.builder)
      }
      // Jenkins dependencies aren't up to date enough to accept this flag.
      // Temporarily exclude until we fully move to GHA.
      if (!ext.project.jenkins.isCIBuild) {
        buildCommandLine.addAll('--provenance=false')
      }
    } else {
      buildCommandLine.add 'build'
      // TARGETOS and TARGETARCH args not present through `docker build`, add here
      ext.buildArgs.put('TARGETOS', 'linux')
      ext.buildArgs.put('TARGETARCH', ext.project.nativeArchitecture())
    }
    if (ext.noCache) {
      buildCommandLine.add '--no-cache'
    }
    if (ext.getNetwork() != null) {
      buildCommandLine.addAll('--network', ext.network)
    }
    if (!ext.buildArgs.isEmpty()) {
      for (Map.Entry<String, String> buildArg : ext.buildArgs.entrySet()) {
        buildCommandLine.addAll('--build-arg', "${buildArg.getKey()}=${buildArg.getValue()}" as String)
      }
    }
    if (!ext.labels.isEmpty()) {
      for (Map.Entry<String, String> label : ext.labels.entrySet()) {
        if (!label.getKey().matches(LABEL_KEY_PATTERN)) {
          throw new GradleException(String.format("Docker label '%s' contains illegal characters. " +
          "Label keys must only contain lowercase alphanumberic, `.`, or `-` characters (must match %s).",
          label.getKey(), LABEL_KEY_PATTERN.pattern()))
        }
        buildCommandLine.addAll('--label', "${label.getKey()}=${label.getValue()}" as String)
      }
    }
    if (ext.pull) {
      buildCommandLine.add '--pull'
    }
    if (!ext.tags.isEmpty() && ext.push) {
      String[] repoParts = (ext.name as String).split(':')
      String repo = repoParts[0]
      for (int i = 1; i < repoParts.length - 1; i++) {
        repo += ':' + repoParts[i]
      }
      for (tag in ext.getTags()) {
        buildCommandLine.addAll(['-t', repo + ':' + tag])
      }
      buildCommandLine.add '.'
    } else {
      buildCommandLine.addAll(['-t', "${-> ext.name}", '.'])
    }
    logger.debug("${buildCommandLine}" as String)
    return buildCommandLine
  }

  private static String computeName(String name, String tag) {
    int firstAt = tag.indexOf("@")

    String tagValue
    if (firstAt > 0) {
      tagValue = tag.substring(firstAt + 1, tag.length())
    } else {
      tagValue = tag
    }

    if (tagValue.contains(':') || tagValue.contains('/')) {
      // tag with ':' or '/' -> force use the tag value
      return tagValue
    } else {
      // tag without ':' and '/' -> replace the tag part of original name
      int lastColon = name.lastIndexOf(':')
      int lastSlash = name.lastIndexOf('/')

      int endIndex;

      // image_name -> this should remain
      // host:port/image_name -> this should remain.
      // host:port/image_name:v1 -> v1 should be replaced
      if (lastColon > lastSlash) endIndex = lastColon
      else endIndex = name.length()

      return name.substring(0, endIndex) + ":" + tagValue
    }
  }

  private static String generateTagTaskName(String name) {
    String tagTaskName = name
    int firstAt = name.indexOf("@")

    if (firstAt > 0) {
      // Get substring of task name
      tagTaskName = name.substring(0, firstAt)
    } else if (firstAt == 0) {
      // Task name must not be empty
      throw new GradleException("Task name of docker tag '${name}' must not be empty.")
    } else if (name.contains(':') || name.contains('/')) {
      // Tags which with repo or name must have a task name
      throw new GradleException("Docker tag '${name}' must have a task name.")
    }

    StringBuffer sb = new StringBuffer(tagTaskName)
    // Uppercase the first letter of task name
    sb.replace(0, 1, tagTaskName.substring(0, 1).toUpperCase());
    return sb.toString()
  }
}
