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

description = "Apache Beam :: Playground :: Backend"

tasks.register<org.gradle.api.tasks.Exec>("format") {
    group = "build"
    description = "Format backend go code"
    executable = "gofmt"
    args("-w", ".")
}

tasks.register<org.gradle.api.tasks.Exec>("tidy") {
    group = "build"
    description = "Run go mod tidy"
    executable = "go"
    args("mod", "tidy")
}

val test by tasks.registering(org.gradle.api.tasks.Exec::class) {
    group = "verification"
    description = "Test the backend"
    executable = "go"
    args("test", "./...")
}

val cleanTestCache by tasks.registering(org.gradle.api.tasks.Exec::class) {
    executable = "go"
    args("clean", "-testcache")
}

val testWithoutCache by tasks.registering(org.gradle.api.tasks.Exec::class) {
    group = "verification"
    description = "Test the backend"
    dependsOn(cleanTestCache)
    executable = "go"
    args("test", "./...")
}

tasks.register<org.gradle.api.tasks.Exec>("removeUnusedSnippet") {
    executable = "go"
    args(
        "run",
        "cmd/remove_unused_snippets/remove_unused_snippets.go",
        "cleanup",
        "-day_diff", System.getProperty("dayDiff"),
        "-project_id", System.getProperty("projectId"),
        "-namespace", System.getProperty("namespace")
    )
}

tasks.register<org.gradle.api.tasks.Exec>("removeSnippet") {
    executable = "go"
    args(
        "run",
        "cmd/remove_unused_snippets/remove_unused_snippets.go",
        "remove",
        "-snippet_id", System.getProperty("snippetId"),
        "-project_id", System.getProperty("projectId"),
        "-namespace", System.getProperty("namespace")
    )
}

tasks.register<org.gradle.api.tasks.Exec>("benchmarkPrecompiledObjects") {
    group = "verification"
    description = "Run benchmarks for precompiled objects"
    executable = "go"
    args("test", "-bench", ".", "-benchmem", "./internal/cloud_bucket/...")
}

tasks.register<org.gradle.api.tasks.Exec>("benchmarkCodeProcessing") {
    group = "verification"
    description = "Run benchmarks for code processing"
    executable = "go"
    args(
        "test",
        "-run=^$",
        "-bench",
        ".",
        "-benchmem",
        "./internal/code_processing/..."
    )
}

tasks.register("benchmark") {
    dependsOn(":playground:backend:benchmarkCodeProcessing")
}

val formatOutput = java.io.ByteArrayOutputStream()

tasks.register<org.gradle.api.tasks.Exec>("checkFormat") {
    executable = "gofmt"
    args("-l", "-e", "-d", ".")
    standardOutput = formatOutput

    doLast {
        if (formatOutput.size() > 0) {
            println(formatOutput.toString())
            throw GradleException(
                "gofmt check indicates that there are unformatted files"
            )
        }
    }
}

tasks.register<org.gradle.api.tasks.Exec>("installLinter") {
    executable = "sh"
    args("env_setup.sh")
}

tasks.register<org.gradle.api.tasks.Exec>("runLint") {
    dependsOn(":playground:backend:installLinter")
    executable = "golangci-lint"
    args("run", "cmd/server/...")
}

tasks.register("precommit") {
    dependsOn(":playground:backend:runLint")
    dependsOn(":playground:backend:checkFormat")
    dependsOn(":playground:backend:tidy")
    dependsOn(":playground:backend:test")
    dependsOn(":playground:backend:benchmark")
}
