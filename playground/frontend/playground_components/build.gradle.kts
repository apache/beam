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

import java.io.ByteArrayOutputStream

description = "Apache Beam :: Playground :: playground_components Flutter Package"

tasks.register("generate") {
  dependsOn("generateCode")
  dependsOn("extractBeamSymbols")
  group = "build"
  description = "Generates all generated files."
}

tasks.register("precommit") {
  dependsOn("analyze")
  dependsOn("test")
  group = "verification"
  description = "All possible checks before a commit."
}

tasks.register<org.gradle.api.tasks.Exec>("analyze") {
  dependsOn("generateCode")
  group = "verification"
  description = "Run dart analyzer"
  commandLine("dart", "analyze", ".")
}

tasks.register<org.gradle.api.tasks.Exec>("test") {
  dependsOn("generateCode")
  group = "verification"
  description = "Run tests"
  commandLine("flutter", "test")
}

tasks.register<org.gradle.api.tasks.Exec>("cleanFlutter") {
  group = "build"
  description = "Remove build artifacts"
  commandLine("flutter", "clean")
}

tasks.register<org.gradle.api.tasks.Exec>("pubGet") {
  group = "build"
  description = "Install dependencies"
  commandLine("flutter", "pub", "get")
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
  group = "build"
  description = "Remove all generated files"

  doLast {
    println("Deleting:")
    deleteFilesByRegExp(".*\\.g\\.dart$")
    deleteFilesByRegExp(".*\\.g\\.yaml$")
    deleteFilesByRegExp(".*\\.gen\\.dart$")
    deleteFilesByRegExp(".*\\.mocks\\.dart$")
  }
}

tasks.register<org.gradle.api.tasks.Exec>("generateCode") {
  dependsOn("cleanFlutter")
  dependsOn("pubGet")
  mustRunAfter("extractBeamSymbols")
  group = "build"
  description = "Generate code"
  commandLine(
    "flutter", "pub", "run", "build_runner", "build",
    "--delete-conflicting-outputs"
  )
}

tasks.register("extractBeamSymbols") {
  dependsOn("ensureSymbolsDirectoryExists")
  dependsOn("extractBeamSymbolsGo")
  dependsOn("extractBeamSymbolsJava")
  dependsOn("extractBeamSymbolsPython")
  group = "build"
  description = "Build Beam symbols dictionaries"
}

tasks.register<org.gradle.api.tasks.Exec>("ensureSymbolsDirectoryExists") {
  commandLine("mkdir", "-p", "assets/symbols")
}

tasks.register<org.gradle.api.tasks.Exec>("extractBeamSymbolsGo") {
  val capturedOutput = ByteArrayOutputStream()

  workingDir("tools/extract_symbols_go")
  commandLine(
    "go", "run", "extract_symbols_go.go",
    "../../../../../sdks/go/pkg/beam"
  )
  standardOutput = capturedOutput

  doLast {
    layout.projectDirectory.file("assets/symbols/go.g.yaml")
      .asFile.writeBytes(capturedOutput.toByteArray())
  }
}

tasks.register("extractBeamSymbolsJava") {
  dependsOn("tools:extract_symbols_java:buildJava")
}

tasks.register<org.gradle.api.tasks.Exec>("extractBeamSymbolsPython") {
  val capturedOutput = ByteArrayOutputStream()

  commandLine(
    "python3",
    "tools/extract_symbols_python/extract_symbols_python.py",
    "../../../sdks/python/apache_beam"
  )
  standardOutput = capturedOutput

  doLast {
    layout.projectDirectory.file("assets/symbols/python.g.yaml")
      .asFile.writeBytes(capturedOutput.toByteArray())
  }
}
