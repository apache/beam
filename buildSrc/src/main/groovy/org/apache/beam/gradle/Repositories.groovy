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

import org.gradle.api.Project

class Repositories {

  static void register(Project project) {

    project.repositories {
      maven { url project.offlineRepositoryRoot }

      // To run gradle in offline mode, one must first invoke
      // 'updateOfflineRepository' to create an offline repo
      // inside the root project directory. See the application
      // of the offline repo plugin within build_rules.gradle
      // for further details.
      if (project.gradle.startParameter.isOffline()) {
        return
      }

      mavenCentral()
      mavenLocal()
      jcenter()

      // Spring only for resolving pentaho dependency.
      maven {
        url "https://repo.spring.io/plugins-release/"
        content { includeGroup "org.pentaho" }
      }

      // Release staging repository
      maven { url "https://oss.sonatype.org/content/repositories/staging/" }

      // Apache nightly snapshots
      maven { url "https://repository.apache.org/snapshots" }

      // Apache release snapshots
      maven { url "https://repository.apache.org/content/repositories/releases" }

      // For Confluent Kafka dependencies
      maven {
        url "https://packages.confluent.io/maven/"
        content { includeGroup "io.confluent" }
      }

      //LYFT CUSTOM pull in the central repo override from settings, if any
      def settingsXml = new File(System.getProperty('user.home'), '.m2/settings.xml')
      if (settingsXml.exists()) {
        def serverId = "lyft-releases"
        def repo = new XmlSlurper().parse(settingsXml).'**'.find { n -> n.name() == 'repository' && serverId.equals(n.id.text()) }
        if (repo) {
          def GroovyShell shell = new GroovyShell(new Binding([env:System.getenv()]))
          maven {
            url shell.evaluate('"' + repo.url.text() +'"')
            name repo.id.text()
            def m2SettingCreds = new XmlSlurper().parse(settingsXml).servers.server.find { server -> serverId.equals(server.id.text()) }
            if (m2SettingCreds) {
              credentials {
                username shell.evaluate('"' + m2SettingCreds.username.text() + '"')
                password shell.evaluate('"' + m2SettingCreds.password.text() + '"')
              }
            }
          }
        }
      }

    }

    // plugin to support repository authentication via ~/.m2/settings.xml
    // https://github.com/mark-vieira/gradle-maven-settings-plugin/
    project.apply plugin: 'net.linguica.maven-settings'

    // Apply a plugin which provides the 'updateOfflineRepository' task that creates an offline
    // repository. This offline repository satisfies all Gradle build dependencies and Java
    // project dependencies. The offline repository is placed within $rootDir/offline-repo
    // but can be overridden by specifying '-PofflineRepositoryRoot=/path/to/repo'.
    // Note that parallel build must be disabled when executing 'updateOfflineRepository'
    // by specifying '--no-parallel', see
    // https://github.com/mdietrichstein/gradle-offline-dependencies-plugin/issues/3
    project.apply plugin: "io.pry.gradle.offline_dependencies"
    project.offlineDependencies {
      repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
        maven { url "https://plugins.gradle.org/m2/" }
        maven { url "https://repo.spring.io/plugins-release" }
        maven { url "https://packages.confluent.io/maven/" }
        maven { url project.offlineRepositoryRoot }
      }
      includeSources = false
      includeJavadocs = false
      includeIvyXmls = false
    }
  }
}

