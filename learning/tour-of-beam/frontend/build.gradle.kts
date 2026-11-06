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

evaluationDependsOn(":playground:frontend:playground_components")

tasks.register("generate") {
  dependsOn(":playground:frontend:playground_components:generate")
  dependsOn("generateCode")
  group = "build"
  description = "Generates all generated files."
}

tasks.register<org.gradle.api.tasks.Exec>("analyze") {
  dependsOn(":playground:frontend:playground_components:generateCode")
  dependsOn("generateCode")
  group = "verification"
  description = "Analyze dart code"
  commandLine("dart", "analyze", "lib", "test")
}

tasks.register<org.gradle.api.tasks.Exec>("pubGet") {
  group = "build"
  description = "Get packages for the frontend project"
  commandLine("flutter", "pub", "get")
}

tasks.register<org.gradle.api.tasks.Exec>("format") {
  group = "build"
  description = "Idiomatically format Dart source code"
  commandLine("dart", "format", "lib", "test")
}

tasks.register<org.gradle.api.tasks.Exec>("run") {
  group = "application"
  description = "Run application on Google Chrome"
  commandLine("flutter", "run", "-d", "chrome")
}

tasks.register<org.gradle.api.tasks.Exec>("test") {
  dependsOn(":playground:frontend:playground_components:generateCode")
  dependsOn("generateCode")
  group = "verification"
  description = "flutter test"
  commandLine("flutter", "test")
}

tasks.register("precommit") {
  dependsOn(":playground:frontend:playground_components:precommit")
  dependsOn("analyze")
  dependsOn("test")
}

tasks.register<org.gradle.api.tasks.Exec>("generateCode") {
  dependsOn("cleanFlutter")
  dependsOn("pubGet")
  group = "build"
  description = "Generate code"
  commandLine(
    "flutter", "pub", "run", "build_runner", "build",
    "--delete-conflicting-outputs"
  )
}

tasks.register<org.gradle.api.tasks.Exec>("cleanFlutter") {
  group = "build"
  description = "Remove build artifacts"
  commandLine("flutter", "clean")
}

val deleteFilesByRegExp: (String) -> Unit = { re ->
  val regex = Regex(re)
  val matchedFiles = listOf("assets", "lib", "test").flatMap { directory ->
    val base = file(directory)
    if (base.exists()) {
      fileTree(base).files.filter {
        regex.matches(it.relativeTo(base).invariantSeparatorsPath)
      }
    } else {
      emptyList()
    }
  }.distinct()

  matchedFiles.forEach { println(it) }
  delete(matchedFiles)
}

tasks.register("cleanGenerated") {
  dependsOn(":playground:frontend:playground_components:cleanGenerated")
  group = "build"
  description = "Remove build artifacts"

  doLast {
    println("Deleting:")
    deleteFilesByRegExp(".*\\.g\\.dart$")
    deleteFilesByRegExp(".*\\.gen\\.dart$")
    deleteFilesByRegExp(".*\\.mocks\\.dart$")
  }
}

tasks.register("integrationTest") {
  dependsOn("integrationTest_tour_page")
  dependsOn("integrationTest_welcome_page")
}

val integrationDeviceId = providers.gradleProperty("deviceId").getOrElse("chrome")

tasks.register<org.gradle.api.tasks.Exec>("integrationTest_tour_page") {
  commandLine(
    "flutter", "drive",
    "--driver=test_driver/integration_test.dart",
    "--target=integration_test/tour_page_test.dart",
    "--web-launch-url='/'",
    "--device-id=$integrationDeviceId"
  )
}

tasks.register<org.gradle.api.tasks.Exec>("integrationTest_welcome_page") {
  commandLine(
    "flutter", "drive",
    "--driver=test_driver/integration_test.dart",
    "--target=integration_test/welcome_page_test.dart",
    "--web-launch-url='/'",
    "--device-id=$integrationDeviceId"
  )
}
