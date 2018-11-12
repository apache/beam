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

import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.FileTree
import org.gradle.api.publish.maven.MavenPublication

class VendorJavaPlugin implements Plugin<Project> {

  static class VendorJavaPluginConfig {
    String dependency
    List<String> packagesToRelocate
    String intoPackage
    String groupId
    String artifactId
    String version
  }

  def isRelease(Project project) {
    return project.hasProperty('isRelease')
  }

  void apply(Project project) {

    // Deferred configuration so the extension is available; otherwise it
    // needs to happen in a task, but not sure where to attach that
    // task
    project.ext.vendorJava = {
      VendorJavaPluginConfig config = it ? it as VendorJavaPluginConfig : new VendorJavaPluginConfig()

      project.apply plugin: 'com.github.johnrengelman.shadow'

      project.apply plugin: 'java'

      // Register all Beam repositories and configuration tweaks
      Repositories.register(project)

      // Apply a plugin which provides tasks for dependency / property / task reports.
      // See https://docs.gradle.org/current/userguide/project_reports_plugin.html
      // for further details. This is typically very useful to look at the "htmlDependencyReport"
      // when attempting to resolve dependency issues.
      project.apply plugin: "project-report"

      project.dependencies { compile "${config.dependency}" }

      project.shadowJar {
        config.packagesToRelocate.each { srcNamespace ->
          relocate(srcNamespace, "${config.intoPackage}.${srcNamespace}")
        }
        classifier = null
        mergeServiceFiles()
        zip64 true
      }

      project.task('validateVendoring', dependsOn: 'shadowJar') {
        inputs.files project.configurations.shadow.artifacts.files
        doLast {
          project.configurations.shadow.artifacts.files.each {
            FileTree exposedClasses = project.zipTree(it).matching {
              include "**/*.class"
              exclude "org/apache/beam/vendor/**"
              // BEAM-5919: Exclude paths for Java 9 multi-release jars.
              exclude "META-INF/versions/*/module-info.class"
              exclude "META-INF/versions/*/org/apache/beam/vendor/**"
            }
            if (exposedClasses.files) {
              throw new GradleException("$it exposed classes outside of org.apache.beam namespace: ${exposedClasses.files}")
            }
          }
        }
      }

      project.apply plugin: 'maven-publish'

      project.publishing {
        repositories {
          maven {
            name "testPublicationLocal"
            url "file://${project.rootProject.projectDir}/testPublication/"
          }
          maven {
            url(project.properties['distMgmtSnapshotsUrl'] ?: isRelease(project)
                    ? 'https://repository.apache.org/service/local/staging/deploy/maven2'
                    : 'https://repository.apache.org/content/repositories/snapshots')

            // We attempt to find and load credentials from ~/.m2/settings.xml file that a user
            // has configured with the Apache release and snapshot staging credentials.
            // <settings>
            //   <servers>
            //     <server>
            //       <id>apache.releases.https</id>
            //       <username>USER_TOKEN</username>
            //       <password>PASS_TOKEN</password>
            //     </server>
            //     <server>
            //       <id>apache.snapshots.https</id>
            //       <username>USER_TOKEN</username>
            //       <password>PASS_TOKEN</password>
            //     </server>
            //   </servers>
            // </settings>
            def settingsXml = new File(System.getProperty('user.home'), '.m2/settings.xml')
            if (settingsXml.exists()) {
              def serverId = (project.properties['distMgmtServerId'] ?: isRelease(project)
                      ? 'apache.releases.https' : 'apache.snapshots.https')
              def m2SettingCreds = new XmlSlurper().parse(settingsXml).servers.server.find { server -> serverId.equals(server.id.text()) }
              if (m2SettingCreds) {
                credentials {
                  username m2SettingCreds.username.text()
                  password m2SettingCreds.password.text()
                }
              }
            }
          }
        }

        publications {
          mavenJava(MavenPublication) {
            groupId = config.groupId
            artifactId = config.artifactId
            version = config.version
            artifact project.shadowJar
          }
        }
      }
    }
  }

}
