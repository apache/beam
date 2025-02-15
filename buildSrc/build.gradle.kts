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

// Plugins for configuring _this build_ of the module
plugins {
  `java-gradle-plugin`
  groovy
  id("com.diffplug.spotless") version "7.0.2"
}

// Define the set of repositories required to fetch and enable plugins.
repositories {
  maven { url = uri("https://plugins.gradle.org/m2/") }
  maven {
    url = uri("https://repo.spring.io/plugins-release/")
    content { includeGroup("io.spring.gradle") }
  }
  // For obsolete Avro plugin
  maven {
    url = uri("https://jitpack.io")
    content { includeGroup("com.github.davidmc24.gradle-avro-plugin") }
  }
}

// Dependencies on other plugins used when this plugin is invoked
dependencies {
  implementation(gradleApi())
  implementation(localGroovy())
  implementation("gradle.plugin.com.github.johnrengelman:shadow:7.1.1")
  implementation("com.github.spotbugs.snom:spotbugs-gradle-plugin:5.0.14")

  runtimeOnly("com.google.protobuf:protobuf-gradle-plugin:0.8.13")                                         // Enable proto code generation
  runtimeOnly("com.github.davidmc24.gradle.plugin:gradle-avro-plugin:1.9.1")                               // Enable Avro code generation
  runtimeOnly("com.diffplug.spotless:spotless-plugin-gradle:5.6.1")                                        // Enable a code formatting plugin
  runtimeOnly("gradle.plugin.com.dorongold.plugins:task-tree:1.5")                                         // Adds a 'taskTree' task to print task dependency tree
  runtimeOnly("gradle.plugin.com.github.johnrengelman:shadow:7.1.1")                                       // Enable shading Java dependencies
  runtimeOnly("net.linguica.gradle:maven-settings-plugin:0.5")
  runtimeOnly("gradle.plugin.io.pry.gradle.offline_dependencies:gradle-offline-dependencies-plugin:0.5.0") // Enable creating an offline repository
  runtimeOnly("net.ltgt.gradle:gradle-errorprone-plugin:3.1.0")                                            // Enable errorprone Java static analysis
  runtimeOnly("org.ajoberstar.grgit:grgit-gradle:4.1.1")                                                   // Enable website git publish to asf-site branch
  runtimeOnly("com.avast.gradle:gradle-docker-compose-plugin:0.16.12")                                     // Enable docker compose tasks
  runtimeOnly("ca.cutterslade.gradle:gradle-dependency-analyze:1.8.3")                                     // Enable dep analysis
  runtimeOnly("gradle.plugin.net.ossindex:ossindex-gradle-plugin:0.4.11")                                  // Enable dep vulnerability analysis
  runtimeOnly("org.checkerframework:checkerframework-gradle-plugin:0.6.37")                                // Enable enhanced static checking plugin
}

// Because buildSrc is built and tested automatically _before_ gradle
// does anything else, it is not possible to spotlessApply because
// spotlessCheck fails before that. So this hack allows disabling
// the check for the moment of application.
//
// ./gradlew :buildSrc:spotlessApply -PdisableSpotlessCheck=true
val disableSpotlessCheck: String by project
val isSpotlessCheckDisabled = project.hasProperty("disableSpotlessCheck") &&
        disableSpotlessCheck == "true"

spotless {
  isEnforceCheck = !isSpotlessCheckDisabled
  groovy {
    excludeJava()
    greclipse().configFile("greclipse.properties")
  }
  groovyGradle {
    greclipse().configFile("greclipse.properties")
  }
}

gradlePlugin {
  plugins {
    create("beamModule") {
      id = "org.apache.beam.module"
      implementationClass = "org.apache.beam.gradle.BeamModulePlugin"
    }
    create("vendorJava") {
      id = "org.apache.beam.vendor-java"
      implementationClass = "org.apache.beam.gradle.VendorJavaPlugin"
    }
    create("beamJenkins") {
      id = "org.apache.beam.jenkins"
      implementationClass = "org.apache.beam.gradle.BeamJenkinsPlugin"
    }
  }
}
