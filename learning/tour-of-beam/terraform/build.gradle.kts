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
import java.util.regex.Pattern

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

    register<TerraformTask>("terraformDestroy") {
        dependsOn("getRouterHost")
        val pg_router_host = project.extensions.extraProperties["pg_router_host"] as String
        var environment = "unknown"
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
                "destroy",
                "-auto-approve",
                "-lock=false",
                "-var=environment=$environment",
                "-var=pg_router_host=$pg_router_host",
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
}

//Add as terminal command in README

//tasks.register("getCredentials") {
//    var gkeClusterName = ""
//    var gkeClusterZone = ""
//    dependsOn("getGKEClusterName", "getGKEClusterZone")
//    mustRunAfter(":learning:tour-of-beam:terraform:getGKEClusterName", ":learning:tour-of-beam:terraform:getGKEClusterZone")
//    group = "backend-deploy"
//    doLast {
//        exec {
//            commandLine("gcloud", "container", "clusters", "get-credentials", gkeClusterName, "--zone", gkeClusterZone)
//        }
//    }
//}

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

tasks.register("firebaseProjectCreate") {
    group = "frontend-deploy"
    val project_id = project.property("project_id") as String
    description = "Adds Firebase to a project if it doesn't already have Firebase."

    doLast {
        val result = ByteArrayOutputStream()

        exec {
            executable("firebase")
            args("projects:list")
            standardOutput = result
        }
        val output = result.toString().trim()
        if (output.contains(project_id)) {
            println("Firebase is already added to project $project_id.")
        } else {
            exec {
                executable("firebase")
                args("projects:addfirebase", project_id)
            }.assertNormalExitValue()
            println("Firebase has been added to project $project_id.")
        }
    }
}


tasks.register("firebaseWebAppCreate") {
    group = "frontend-deploy"
    val project_id = project.property("project_id") as String
    val webapp_id = project.property("webapp_id") as String
    doLast {
        println(project_id)
        println(webapp_id)
        val result = ByteArrayOutputStream()

        exec {
            executable("firebase")
            args("apps:list", "--project", project_id)
            standardOutput = result
        }
        val output = result.toString().trim()
        if (output.contains(webapp_id)) {
            println("Tour of Beam Web App $webapp_id is already created on the project $project_id.")
            val firebaseAppId = result.toString().lines().find { it.contains("1:") }?.substringAfter("$webapp_id │ ")?.substringBefore(" │ WEB")?.trim()
            project.extensions.extraProperties["firebaseAppId"] = firebaseAppId
            } else {
                val result2 = ByteArrayOutputStream()
                exec {
                    executable("firebase")
                    args("apps:create", "WEB", webapp_id, "--project", project_id)
                    standardOutput = result2
                    }.assertNormalExitValue()
                    val firebaseAppId = result2.toString().lines().find { it.startsWith("  - App ID:") }?.substringAfter(":")?.trim()
                    project.extensions.extraProperties["firebaseAppId"] = firebaseAppId
                    println("Firebase app ID for newly created Firebase Web App: $firebaseAppId")
            }
    }
}

// firebase apps:sdkconfig WEB 1:11155893632:web:09743665f1f2d7cb086565
tasks.register("getSdkConfigWebApp") {
    group = "frontend-deploy"
    val firebaseAppId = project.extensions.extraProperties["firebaseAppId"] as String
    val result = ByteArrayOutputStream()
    doLast{
        exec {
            executable("firebase")
            args("apps:sdkconfig", "WEB", firebaseAppId)
            standardOutput = result
        }
        val output = result.toString().trim()
        val pattern = Pattern.compile("\\{.*\"locationId\":\\s*\"(.*?)\".*\\}", Pattern.DOTALL)
        val matcher = pattern.matcher(output)
        if (matcher.find()) {
            val firebaseConfigData = matcher.group().replace("{", "").replace("}", "")
            project.extensions.extraProperties["firebaseConfigData"] = firebaseConfigData
            println("Firebase config data: $firebaseConfigData")
        } else {
            throw Exception("Unable to extract Firebase config data from output.")
        }
    }
}

tasks.register("prepareFirebaseOptionsDart") {
    group = "frontend-deploy"
    doLast {
        val firebaseConfigData = project.extensions.extraProperties["firebaseConfigData"] as String
        val file = project.file("../frontend/lib/firebase_options.dart")
        val content = file.readText()
        val updatedContent = content.replace(Regex("""FirebaseOptions\((.*)\)"""), "FirebaseOptions(${firebaseConfigData})")
        file.writeText(updatedContent)
    }
}

tasks.register("flutterPubGetPG") {
    exec {
    commandLine("flutter", "pub", "get")
    workingDir("../../../playground/frontend/playground_components")
    }
}

tasks.register("flutterPubRunPG") {
    exec {
        executable("flutter")
        args("pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
        workingDir("../../../playground/frontend/playground_components")
    }
}

tasks.register("flutterPubGetTob") {
    exec {
        commandLine("flutter", "pub", "get")
        workingDir("../frontend")
    }
}

tasks.register("flutterPubRunTob") {
    exec {
        commandLine("flutter", "pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
        workingDir("../frontend")
    }
}

tasks.register("flutterBuildWeb") {
    exec {
        commandLine("flutter", "build", "web", "--profile", "--dart-define=Dart2jsOptimization=O0")
        workingDir("../frontend")
    }
}

tasks.register("firebaseDeploy") {
    var project_id = ""
    if (project.hasProperty("project_id")) {
        project_id = project.property("project_id") as String
    }
    exec {
        commandLine("firebase", "deploy", "--project", project_id)
        workingDir("../frontend")
    }
}

tasks.register("prepareConfig") {
    group = "frontend-deploy"
    doLast {
        var dns_name = ""
        var region = ""
        var project_id = ""
        if (project.hasProperty("region")) {
            region = project.property("region") as String
        }
        if (project.hasProperty("project_id")) {
            project_id = project.property("project_id") as String
        }
        if (project.hasProperty("dns-name")) {
            dns_name = project.property("dns-name") as String
        }
        val configFileName = "config.dart"
        val modulePath = project(":learning:tour-of-beam:frontend").projectDir.absolutePath
        val file = File("$modulePath/lib/$configFileName")

        file.writeText(
                """
const _cloudFunctionsProjectRegion = '$region';
const _cloudFunctionsProjectId = '$project_id';
const cloudFunctionsBaseUrl = 'https://'
    '$region-$project_id'
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


// Should be as CI CD process

tasks.register("populateDatastore") {
    group = "backend-deploy"
    var project_id = "unknown"
    if (project.hasProperty("project_id")) {
        project_id = project.property("project_id") as String
    }
    doLast {
        val result: ExecResult = project.exec {
            commandLine("go", "run", "cmd/ci_cd/ci_cd.go")
            environment("DATASTORE_PROJECT_ID", project_id)
            environment("GOOGLE_PROJECT_ID", project_id)
            environment("TOB_LEARNING_ROOT", "../learning-content/")
            workingDir("../backend")
        }
        if (result.exitValue != 0) {
            throw GradleException("Command execution failed with exit code ${result.exitValue}")
        }
        println("Output of script:\n$result")
    }
}


/* Tour of Beam backend init */
    tasks.register("InitBackend") {
    group = "backend-deploy"
    description = "ToB Backend Init"
    val getRouterHost = tasks.getByName("getRouterHost")
    val indexCreate = tasks.getByName("indexcreate")
    val tfInit = tasks.getByName("terraformInit")
    val tfApplyBackend = tasks.getByName("terraformApplyBackend")
    dependsOn(getRouterHost)
    Thread.sleep(3000)
    dependsOn(indexCreate)
    dependsOn(tfInit)
    dependsOn(tfApplyBackend)
    indexCreate.mustRunAfter(getRouterHost)
    tfInit.mustRunAfter(indexCreate)
    tfApplyBackend.mustRunAfter(tfInit)
}

tasks.register("destroyBackend") {
    group = "backend-destroy"
    description = "ToB Backend Destroy"
    val getRouterHost = tasks.getByName("getRouterHost")
    val terraformDestroy = tasks.getByName("terraformDestroy")
    dependsOn(getRouterHost)
    Thread.sleep(3000)
    dependsOn(terraformDestroy)
    terraformDestroy.mustRunAfter(getRouterHost)
}

tasks.register("InitFrontend") {
    group = "frontend-deploy"
    description = "ToB Frontend Init"
    val prepareConfig = tasks.getByName("prepareConfig")
    val firebaseProjectCreate = tasks.getByName("firebaseProjectCreate")
    val firebaseWebAppCreate = tasks.getByName("firebaseWebAppCreate")
    val getSdkConfigWebApp = tasks.getByName("getSdkConfigWebApp")
    val prepareFirebaseOptionsDart = tasks.getByName("prepareFirebaseOptionsDart")
    val flutterPubGetPG = tasks.getByName("flutterPubGetPG")
    val flutterPubRunPG = tasks.getByName("flutterPubRunPG")
    val flutterPubGetTob = tasks.getByName("flutterPubGetTob")
    val flutterPubRunTob = tasks.getByName("flutterPubRunTob")
    val flutterBuildWeb = tasks.getByName("flutterBuildWeb")
    val firebaseDeploy = tasks.getByName("firebaseDeploy")
    dependsOn(prepareConfig)
    Thread.sleep(5000)
    dependsOn(firebaseProjectCreate)
    Thread.sleep(9000)
    dependsOn(firebaseWebAppCreate)
    Thread.sleep(9000)
    dependsOn(getSdkConfigWebApp)
    dependsOn(prepareFirebaseOptionsDart)
    dependsOn(flutterPubGetPG)
    dependsOn(flutterPubRunPG)
    dependsOn(flutterPubGetTob)
    dependsOn(flutterPubRunTob)
    dependsOn(flutterBuildWeb)
    dependsOn(firebaseDeploy)
    firebaseProjectCreate.mustRunAfter(prepareConfig)
    firebaseWebAppCreate.mustRunAfter(firebaseProjectCreate)
    getSdkConfigWebApp.mustRunAfter(firebaseWebAppCreate)
    prepareFirebaseOptionsDart.mustRunAfter(getSdkConfigWebApp)
    flutterPubGetPG.mustRunAfter(prepareFirebaseOptionsDart)
    flutterPubRunPG.mustRunAfter(flutterPubGetPG)
    flutterPubGetTob.mustRunAfter(flutterPubRunPG)
    flutterPubRunTob.mustRunAfter(flutterPubGetTob)
    flutterBuildWeb.mustRunAfter(flutterPubRunTob)
    firebaseDeploy.mustRunAfter(flutterBuildWeb)
}


