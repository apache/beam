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

    register<TerraformTask>("terraformApplyBackend") {
        group = "backend-deploy"
        var environment = ""
        val pg_router_host = project.extensions.extraProperties["pg_router_host"] as String
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
                "apply",
                "-auto-approve",
                "-lock=false",
                "-var=pg_router_host=$pg_router_host",
                "-target=module.api_enable",
                "-target=module.setup",
                "-target=module.functions_buckets",
                "-target=module.cloud_functions",
                "-var=environment=$environment",
                if (file("./environment/$environment/terraform.tfvars").exists()) {
                    "-var-file=./environment/$environment/terraform.tfvars"
                } else {
                    "-no-color"
                }
        )
    }

    register<TerraformTask>("terraformApplyFrontend") {
        var environment = "unknown"
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
                "apply",
                "-auto-approve",
                "-lock=false",
                "-var=environment=$environment",
                "-target=module.firebase",
                if (file("./environment/$environment/terraform.tfvars").exists()) {
                    "-var-file=./environment/$environment/terraform.tfvars"
                } else {
                    "-no-color"
                }
        )
    }
}

tasks.register("getGKEClusterName") {
    group = "backend-deploy"
    val result = ByteArrayOutputStream()
    exec {
        commandLine("gcloud", "container", "clusters", "list", "--format=value(name)")
        standardOutput = result
    }
    val gkeClusterName = result.toString().trim()
    project.extensions.extraProperties["gkeClusterName"] = gkeClusterName
}

tasks.register("getGKEClusterZone") {
    group = "backend-deploy"
    val result = ByteArrayOutputStream()
    exec {
        commandLine("gcloud", "container", "clusters", "list", "--format=value(zone)")
        standardOutput = result
    }
    val gkeClusterZone = result.toString().trim()
    project.extensions.extraProperties["gkeClusterZone"] = gkeClusterZone
}

tasks.register("getCredentials") {
    dependsOn("getGKEClusterName", "getGKEClusterZone")
    mustRunAfter(":learning:tour-of-beam:terraform:getGKEClusterName", ":learning:tour-of-beam:terraform:getGKEClusterZone")
    group = "backend-deploy"
    val gkeClusterZone = project.extensions.extraProperties["gkeClusterZone"] as String
    val gkeClusterName = project.extensions.extraProperties["gkeClusterName"] as String
    doLast {
        exec {
            commandLine("gcloud", "container", "clusters", "get-credentials", gkeClusterName, "--zone", gkeClusterZone)
        }
    }
}

tasks.register("getRouterHost") {
    group = "backend-deploy"
    val result = ByteArrayOutputStream()
    exec {
        commandLine("kubectl", "get", "svc", "-l", "app=backend-router-grpc", "-o", "jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}:{.items[0].spec.ports[0].port}'")
        standardOutput = result
    }
    val pg_router_host = result.toString().trim().replace("'", "")
    project.extensions.extraProperties["pg_router_host"] = pg_router_host
    }

tasks.register("indexcreate") {
    group = "backend-deploy"
    val indexpath = "../backend/internal/storage/index.yaml"
    doLast{
        exec {
            executable("gcloud")
            args("datastore", "indexes", "create", indexpath)
        }
    }
}

tasks.register("populateDatastore") {
    group = "backend-deploy"
    val result = ByteArrayOutputStream()
    doLast {
        var projectId = "unknown"
        if (project.hasProperty("projectId")) {
            projectId = project.property("projectId") as String
        }
        System.setProperty("DATASTORE_PROJECT_ID", projectId)
        System.setProperty("GOOGLE_PROJECT_ID", projectId)
        System.setProperty("TOB_LEARNING_ROOT", "../learning-content/")

        exec {
        commandLine("go", "../backend/cmd/ci_cd/ci_cd.go")
        standardOutput = result
        }
        println("Output of go run cmd/ci_cd/ci_cd.go command: $result")
    }
}

        tasks.register("prepareConfig") {
            group = "frontend-deploy"
            doLast {
                var dns_name = ""
                var region = ""
                var projectId = ""
                if (project.hasProperty("region")) {
                    region = project.property("region") as String
                }
                if (project.hasProperty("projectId")) {
                    projectId = project.property("projectId") as String
                }
                if (project.hasProperty("dns-name")) {
                    dns_name = project.property("dns-name") as String
                }
                val configFileName = "config.g.dart"
                val modulePath = project(":learning:tour-of-beam:frontend").projectDir.absolutePath
                var file = File("$modulePath/lib/$configFileName")

                file.writeText(
                        """
const _cloudFunctionsProjectRegion = '$region';
const _cloudFunctionsProjectId = '$projectId';
const cloudFunctionsBaseUrl = 'https://'
    '$region-$projectId'
    '.cloudfunctions.net';


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

/* Tour of Beam backend init */
tasks.register("InitBackend") {
    group = "backend-deploy"
    description = "ToB Backend Init"
    val getGkeName = tasks.getByName("getGKEClusterName")
    val getGkeZone = tasks.getByName("getGKEClusterZone")
    val getCreds = tasks.getByName("getCredentials")
    val getRouterHost = tasks.getByName("getRouterHost")
    val indexCreate = tasks.getByName("indexcreate")
    val tfInit = tasks.getByName("terraformInit")
    val tfApplyBackend = tasks.getByName("terraformApplyBackend")
    val initDatastore = tasks.getByName("populateDatastore")
    dependsOn(getGkeName)
    Thread.sleep(9000)
    dependsOn(getGkeZone)
    Thread.sleep(9000)
    dependsOn(getCreds)
    Thread.sleep(9000)
    dependsOn(getRouterHost)
    Thread.sleep(9000)
    dependsOn(indexCreate)
    dependsOn(tfInit)
    dependsOn(tfApplyBackend)
    dependsOn(initDatastore)
    getGkeZone.mustRunAfter(getGkeName)
    getCreds.mustRunAfter(getGkeZone)
    getRouterHost.mustRunAfter(getCreds)
    indexCreate.mustRunAfter(getRouterHost)
    tfInit.mustRunAfter(indexCreate)
    tfApplyBackend.mustRunAfter(tfInit)
    initDatastore.mustRunAfter(tfApplyBackend)
}
