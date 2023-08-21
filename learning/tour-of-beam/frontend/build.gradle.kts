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

tasks.register("analyze") {
  dependsOn(":playground:frontend:playground_components:generateCode")
  dependsOn("generateCode")

  group = "verification"
  description = "Analyze dart code"

  doLast {
    exec {
      executable("dart")
      args("analyze", "lib", "test")
    }
  }
}

tasks.register("pubGet") {
  group = "build"
  description = "Get packages for the frontend project"
  doLast {
    exec {
      executable("flutter")
      args("pub", "get")
    }
  }
}

tasks.register("format") {
  group = "build"
  description = "Idiomatically format Dart source code"
  doLast {
    exec {
      executable("dart")
      args("format", "lib", "test")
    }
  }
}

tasks.register("run") {
  group = "application"
  description = "Run application on Google Chrome"

  doLast {
    exec {
      executable("flutter")
      args("run", "-d", "chrome")
    }
  }
}

tasks.register("test") {
  dependsOn(":playground:frontend:playground_components:generateCode")
  dependsOn("generateCode")

  group = "verification"
  description = "flutter test"

  doLast {
    exec {
      executable("flutter")
      args("test")
    }
  }
}

tasks.register("precommit") {
  dependsOn(":playground:frontend:playground_components:precommit")

  dependsOn("analyze")
  dependsOn("test")
}

tasks.register("generateCode") {
  dependsOn(":playground:frontend:playground_components:generateCode")

  dependsOn("cleanFlutter")
  dependsOn("pubGet")

  group = "build"
  description = "Generate code"

  doLast {
    exec {
      executable("flutter")
      args("pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
    }
  }
}

tasks.register("cleanFlutter") {
  group = "build"
  description = "Remove build artifacts"

  doLast {
    exec {
      executable("flutter")
      args("clean")
    }
  }
}

tasks.register("cleanGenerated") {
  dependsOn(":playground:frontend:playground_components:cleanGenerated")

  group = "build"
  description = "Remove build artifacts"

  doLast {
    println("Deleting:")

    deleteFilesByRegExp(".*\\.g\\.dart\$")
    deleteFilesByRegExp(".*\\.gen\\.dart\$")
    deleteFilesByRegExp(".*\\.mocks\\.dart\$")
  }
}

val deleteFilesByRegExp: (String) -> Unit = { re ->
  // Prints file names.
  exec {
    executable("find")
    args("assets", "lib", "test", "-regex", re)
  }
  // Actually deletes them.
  exec {
    executable("find")
    args("assets", "lib", "test", "-regex", re, "-delete")
  }
}

tasks.register("integrationTest") {
  dependsOn("integrationTest_tour_page")
  dependsOn("integrationTest_welcome_page")
}

tasks.register("integrationTest_tour_page") {
  doLast {
    runIntegrationTest("tour_page", "/")
  }
}

tasks.register("integrationTest_welcome_page") {
  doLast {
    runIntegrationTest("welcome_page", "/")
  }
}

fun runIntegrationTest(path: String, url: String) {
  // Run with -PdeviceId=web-server for headless tests.
  val deviceId: String = if (project.hasProperty("deviceId")) project.property("deviceId") as String else "chrome"

  exec {
    executable = "flutter"
    args(
      "drive",
      "--driver=test_driver/integration_test.dart",
      "--target=integration_test/${path}_test.dart",
      "--web-launch-url='$url'",
      "--device-id=$deviceId",
    )
  }
}
