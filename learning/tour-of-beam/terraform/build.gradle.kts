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

import com.pswidersk.gradle.terraform.TerraformTask
import java.io.ByteArrayOutputStream

plugins {
    id("com.pswidersk.terraform-plugin") version "1.0.0"
}

terraformPlugin {
    terraformVersion.set("1.0.9")
}

tasks.register("prepareConfig") {
    group = "deploy"
    doLast {
        var dns_name = ""
        if (project.hasProperty("dns-name")) {
            dns_name = project.property("dns-name") as String
        }
        val configFileName = "config.g.dart"
        val modulePath = project(":learning:tour-of-beam:frontend").projectDir.absolutePath
        var file = File("$modulePath/lib/$configFileName")

        file.writeText(
                """
const String kAnalyticsUA = 'UA-73650088-2';
const String kApiClientURL =
      'https://router.${dns_name}';
const String kApiJavaClientURL =
      'https://java.${dns_name}';
const String kApiGoClientURL =
      'https://go.${dns_name}';
const String kApiPythonClientURL =
      'https://python.${dns_name}';
const String kApiScioClientURL =
      'https://scio.${dns_name}';
"""
        )
    }
}

tasks {
    /* init Infrastructure for migrate */
    register<TerraformTask>("terraformInit") {
        // exec args can be passed by commandline, for example
        var environment = "unknown"
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
                "init", "-migrate-state",
                "-backend-config=./environment/$environment/state.tfbackend",
                "-var=environment=$environment",
                if (file("./environment/$environment/terraform.tfvars").exists()) {
                    "-var-file=./environment/$environment/terraform.tfvars"
                } else {
                    "-no-color"
                }
        )
    }

    /* refresh Infrastucture for remote state */
    register<TerraformTask>("terraformRef") {
        mustRunAfter(":learning:tour-of-beam:terraform:terraformInit")
        var environment = "unknown"
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
                "refresh",
                "-lock=false",
                "-var=environment=$environment",
                if (file("./environment/$environment/terraform.tfvars").exists()) {
                    "-var-file=./environment/$environment/terraform.tfvars"
                } else {
                    "-no-color"
                }
        )
    }

    register<TerraformTask>("terraformApply") {
        var environment = "unknown"
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
          else {
            environment
          }
        args(
                "apply",
                "-auto-approve",
                "-lock=false",
                "-var=environment=$environment",
                if (file("./environment/$environment/terraform.tfvars").exists()) {
                    "-var-file=./environment/$environment/terraform.tfvars"
                } else {
                    "-no-color"
                }
        )
    }
}

tasks.register("getCredentials") {
    var gke_cluster_name = "unknown"
    var gke_zone = "unknown"
    var project_id = "unknown"
    if (project.hasProperty("gke_cluster_name")) {
        gke_cluster_name = project.property("gke_cluster_name") as String
    }
    if (project.hasProperty("gke_zone")) {
        gke_zone = project.property("gke_cluster_name") as String
    }
    if (project.hasProperty("project_id")) {
        project_id = project.property("project_id") as String
    }
    doLast{
        exec {
            executable("gcloud")
            args("container", "clusters", "get-credentials", "$gke_cluster_name", "--zone $gke_zone", "--project $project_id")
        }
    }
}

tasks.register("getRouterHost") {
    var playground_router_host = ""
    var stdout = ByteArrayOutputStream()
    doLast{
        val result = exec {
            executable("kubectl")
            args("get", "svc", "-l", "app=backend-router-grpc", "-o", "jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}:{.items[0].spec.ports[0].port}'")
            standardOutput = stdout
        }
        playground_router_host = stdout.toString().trim().replace("\"", "")
        stdout = ByteArrayOutputStream()
    }
}

tasks.register("indexcreate") {
    group = "deploy"
    val indexpath = "../backend/internal/storage/index.yaml"
    doLast{
        exec {
            executable("gcloud")
            args("datastore", "indexes", "create", indexpath)
        }
    }
}

tasks.register("populateDatastore") {
    var environment = "unknown"
    if (project.hasProperty("project_environment")) {
        environment = project.property("project_environment") as String
    }
    doLast {
        val file = File("./environment/$environment/terraform.tfvars")
        val lines = file.readLines()
        for (line in lines) {
            val pattern = "project_id = '(.*?)'".toRegex()
            val matchResult = pattern.find(line)
            if (matchResult != null) {
                val value = matchResult.groupValues[1]
                System.setProperty("DATASTORE_PROJECT_ID", value)
                System.setProperty("GOOGLE_PROJECT_ID", value)
        System.setProperty("TOB_LEARNING_ROOT", "../learning-content/")

        val process = Runtime.getRuntime().exec(arrayOf("bash", "-c", "go ../backend/cmd/ci_cd/ci_cd.go"))
        val output = process.inputStream.bufferedReader().use {
            it.readText().trim()
        }
        println("Output of go run cmd/ci_cd/ci_cd.go command: $output")
    }
}

// command to run
// ./gradlew runGoCommand \
// -PdatastoreProjectId=my-datastore-project-id \
// -PgoogleProjectId=my-google-project-id \
// -PtobLearningRoot=path/to/tob-learning-root

tasks.register("readState") {
    group = "deploy"
    dependsOn(":playground:terraform:terraformInit")
    dependsOn(":playground:terraform:terraformRef")
}

/* initialization infrastructure */
tasks.register("InitInfrastructure") {
    group = "deploy"
    description = "initialization infrastructure"
    val init = tasks.getByName("terraformInit")
    val apply = tasks.getByName("terraformApply")
    val prepare = tasks.getByName("prepareConfig")
    dependsOn(init)
    dependsOn(apply)
    dependsOn(prepare)
    apply.mustRunAfter(init)
    prepare.mustRunAfter(apply)
}