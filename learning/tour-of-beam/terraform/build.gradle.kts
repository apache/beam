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
    terraformVersion.set("1.4.2")
}

/* init Infrastructure for migrate */
tasks.register<TerraformTask>("terraformInit") {
    // exec args can be passed by commandline, for example
    args(
        "init", "-migrate-state",
        "-backend-config=./state.tfbackend"
    )
}

/* refresh Infrastucture for remote state */
tasks.register<TerraformTask>("terraformRef") {
    args(
        "refresh",
        "-lock=false",
        "-var-file=./common.tfvars"
    )
}

tasks.register<TerraformTask>("terraformApplyBackend") {
    group = "backend-deploy"
    var pg_router_host = project.extensions.extraProperties["pg_router_host"] as String
    args(
        "apply",
        "-auto-approve",
        "-lock=false",
        "-parallelism=3",
        "-var=pg_router_host=$pg_router_host",
        "-var=gcloud_init_account=$(gcloud config get-value core/account)",
        "-var=project_id=$(gcloud config get-value project)",
        "-var-file=./common.tfvars"
    )

    tasks.getByName("uploadLearningMaterials").mustRunAfter(this)

}

tasks.register<TerraformTask>("terraformDestroy") {
    var pg_router_host = project.extensions.extraProperties["pg_router_host"] as String
    args(
        "destroy",
        "-auto-approve",
        "-lock=false",
        "-var=pg_router_host=$pg_router_host",
        "-var=gcloud_init_account=$(gcloud config get-value core/account)",
        "-var=project_id=$(gcloud config get-value project)",
        "-var-file=./common.tfvars"
    )
}

tasks.register("getRouterHost") {
    doLast {
        group = "backend-deploy"
        val result = ByteArrayOutputStream()
        exec {
            commandLine(
                "kubectl",
                "get",
                "svc",
                "-l",
                "app=backend-router-grpc",
                "-o",
                "jsonpath='{.items[0].status.loadBalancer.ingress[0].ip}:{.items[0].spec.ports[0].port}'"
            )
            standardOutput = result
        }
        val pg_router_host = result.toString().trim().replace("'", "")
        project.extensions.extraProperties["pg_router_host"] = pg_router_host
    }
}

tasks.register("indexcreate") {
    doLast {
        group = "backend-deploy"
        val indexpath = "../backend/internal/storage/index.yaml"
        exec {
            executable("gcloud")
            args("datastore", "indexes", "create", indexpath)
        }
    }
}

tasks.register("firebaseProjectCreate") {
    doLast {
        group = "frontend-deploy"
        val result = ByteArrayOutputStream()
        var project_id = project.property("project_id") as String
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
    doLast {
        group = "frontend-deploy"
        val result = ByteArrayOutputStream()
        var project_id = project.property("project_id") as String
        var webapp_id = project.property("webapp_id") as String
        exec {
            executable("firebase")
            args("apps:list", "--project", project_id)
            standardOutput = result
        }
        println(result)
        val output = result.toString()
        if (output.contains(webapp_id)) {
            println("Webapp id $webapp_id is already created on the project: $project_id.")
            val regex = Regex("$webapp_id[│ ]+([\\w:]+)[│ ]+WEB[│ ]+")
            val firebaseAppId = regex.find(output)?.groupValues?.get(1)?.trim()
            project.extensions.extraProperties["firebaseAppId"] = firebaseAppId
        } else {
            val result2 = ByteArrayOutputStream()
            exec {
                executable("firebase")
                args("apps:create", "WEB", webapp_id, "--project", project_id)
                standardOutput = result2
            }.assertNormalExitValue()
            val firebaseAppId =
                result2.toString().lines().find { it.startsWith("  - App ID:") }?.substringAfter(":")?.trim()
            project.extensions.extraProperties["firebaseAppId"] = firebaseAppId
            println("Firebase app ID for newly created Firebase Web App: $firebaseAppId")
        }
    }
}

// firebase apps:sdkconfig WEB AppId
tasks.register("getSdkConfigWebApp") {
    doLast {
        group = "frontend-deploy"
        val firebaseAppId = project.extensions.extraProperties["firebaseAppId"] as String
        val result = ByteArrayOutputStream()
        exec {
            executable("firebase")
            args("apps:sdkconfig", "WEB", firebaseAppId)
            standardOutput = result
        }
        val output = result.toString().trim()
        val pattern = Pattern.compile("\\{[^{]*\"locationId\":\\s*\".*?\"[^}]*\\}", Pattern.DOTALL)
        val matcher = pattern.matcher(output)
        if (matcher.find()) {
            val firebaseConfigData = matcher.group().replace("{", "")
                .replace("}", "")
                .replace("\"locationId\":\\s*\".*?\",?".toRegex(), "")
                .replace("\"(\\w+)\":".toRegex(), "$1:")
                .replace(":\\s*\"(.*?)\"".toRegex(), ":\"$1\"")
            project.extensions.extraProperties["firebaseConfigData"] = firebaseConfigData.trim()
            println("Firebase config data: $firebaseConfigData")
        }
    }
}

tasks.register("prepareFirebaseOptionsDart") {
    doLast {
        group = "frontend-deploy"
        val firebaseConfigData = project.extensions.extraProperties["firebaseConfigData"] as String
        val file = project.file("../frontend/lib/firebase_options.dart")
        val content = file.readText()
        val updatedContent = content.replace(
            Regex("""static const FirebaseOptions web = FirebaseOptions\(([^)]+)\);"""),
            "static const FirebaseOptions web = FirebaseOptions(${firebaseConfigData});"
        )
        file.writeText(updatedContent)
    }
}

tasks.register("flutterPubGetPG") {
    doLast {
        exec {
            executable("flutter")
            args("pub", "get")
            workingDir("../../../playground/frontend/playground_components")
        }
    }
}

tasks.register("flutterPubRunPG") {
    doLast {
        exec {
            executable("flutter")
            args("pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
            workingDir("../../../playground/frontend/playground_components")
        }
    }
}

tasks.register("flutterPubGetTob") {
    doLast {
        exec {
            executable("flutter")
            args("pub", "get")
            workingDir("../frontend")
        }
    }
}

tasks.register("flutterPubRunTob") {
    doLast {
        exec {
            executable("flutter")
            args("pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
            workingDir("../frontend")
        }
    }
}

tasks.register("flutterBuildWeb") {
    doLast {
        exec {
            executable("flutter")
            args("build", "web", "--profile", "--dart-define=Dart2jsOptimization=O0")
            workingDir("../frontend")
        }
    }
}

tasks.register("firebaseDeploy") {
    doLast {
        var project_id = project.property("project_id") as String
        exec {
            commandLine("firebase", "deploy", "--project", project_id)
            workingDir("../frontend")
        }
    }
}

tasks.register("prepareConfig") {
    doLast {
        group = "frontend-deploy"
        var region = project.property("region") as String
        var project_id = project.property("project_id") as String
        var environment = project.property("project_environment") as String
        var dns_name = project.property("dns-name") as String
        val configFileName = "config.dart"
        val modulePath = project(":learning:tour-of-beam:frontend").projectDir.absolutePath
        val file = File("$modulePath/lib/$configFileName")

        file.writeText(
            """
const _cloudFunctionsProjectRegion = '$region';
const _cloudFunctionsProjectId = '$project_id';
const cloudFunctionsBaseUrl = 'https://'
    '$region-$project_id'
    '.cloudfunctions.net/${environment}_';


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

tasks.register("prepareFirebasercConfig") {
    doLast {
        group = "frontend-deploy"
        var project_id = project.property("project_id") as String
        val configFileName = ".firebaserc"
        val modulePath = project(":learning:tour-of-beam:frontend").projectDir.absolutePath
        val file = File("$modulePath/$configFileName")

        file.writeText(
            """
{
  "projects": {
    "default": "$project_id"
  }
}
"""
        )
    }
}

tasks.register("uploadLearningMaterials") {
    doLast {
        var project_id = project.property("project_id") as String
        group = "backend-deploy"
        exec {
            commandLine("go", "run", "cmd/ci_cd/ci_cd.go")
            environment("DATASTORE_PROJECT_ID", project_id)
            environment("GOOGLE_PROJECT_ID", project_id)
            environment("TOB_LEARNING_ROOT", "../learning-content/")
            workingDir("../backend")
        }
    }
    dependsOn("terraformApplyBackend")
    mustRunAfter("terraformApplyBackend")
}

/* Tour of Beam backend init */
tasks.register("InitBackend") {
    group = "backend-deploy"
    val getRouterHost = tasks.getByName("getRouterHost")
    val indexCreate = tasks.getByName("indexcreate")
    val tfInit = tasks.getByName("terraformInit")
    val tfApplyBackend = tasks.getByName("terraformApplyBackend")
    val uploadLearningMaterials = tasks.getByName("uploadLearningMaterials")
    dependsOn(getRouterHost)
    dependsOn(indexCreate)
    dependsOn(tfInit)
    dependsOn(tfApplyBackend)
    dependsOn(uploadLearningMaterials)
    indexCreate.mustRunAfter(getRouterHost)
    tfInit.mustRunAfter(indexCreate)
    tfApplyBackend.mustRunAfter(tfInit)
    uploadLearningMaterials.mustRunAfter(tfApplyBackend)
}

tasks.register("DestroyBackend") {
    group = "backend-destroy"
    val getRouterHost = tasks.getByName("getRouterHost")
    val terraformDestroy = tasks.getByName("terraformDestroy")
    dependsOn(getRouterHost)
    dependsOn(terraformDestroy)
    terraformDestroy.mustRunAfter(getRouterHost)
}

tasks.register("InitFrontend") {
    group = "frontend-deploy"
    val prepareConfig = tasks.getByName("prepareConfig")
    val prepareFirebasercConfig = tasks.getByName("prepareFirebasercConfig")
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
    dependsOn(prepareFirebasercConfig)
    dependsOn(firebaseProjectCreate)
    dependsOn(firebaseWebAppCreate)
    dependsOn(getSdkConfigWebApp)
    dependsOn(prepareFirebaseOptionsDart)
    dependsOn(flutterPubGetPG)
    dependsOn(flutterPubRunPG)
    dependsOn(flutterPubGetTob)
    dependsOn(flutterPubRunTob)
    dependsOn(flutterBuildWeb)
    dependsOn(firebaseDeploy)
    prepareFirebasercConfig.mustRunAfter(prepareConfig)
    firebaseProjectCreate.mustRunAfter(prepareFirebasercConfig)
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
