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
    id("com.pswidersk.terraform-plugin") version "1.1.1"
}

terraformPlugin {
    terraformVersion.set("1.4.2")
}

/* init Infrastructure for migrate */
tasks.register<TerraformTask>("terraformInit") {
    dependsOn("indexcreate")

    args(
        "init", "-migrate-state",
        "-backend-config=./state.tfbackend"
    )
}

/* refresh Infrastructure for remote state */
tasks.register<TerraformTask>("terraformRef") {
    args(
        "refresh",
        "-lock=false",
        "-var-file=./common.tfvars"
    )
}

tasks.register<TerraformTask>("terraformApplyBackend") {
    group = "backend-deploy"

    val pg_router_host = if (project.extensions.extraProperties.has("pg_router_host")) {
        project.extensions.extraProperties["pg_router_host"] as String
    } else {
        "unknown"
    }
    args(
        "apply",
        "-auto-approve",
        "-lock=false",
        "-parallelism=3",
        "-var=pg_router_host=$pg_router_host",
        "-var=project_id=$(gcloud config get-value project)",
        "-var-file=./common.tfvars"
    )

    tasks.getByName("uploadLearningMaterials").mustRunAfter(this)
}

tasks.register<TerraformTask>("terraformDestroy") {
    dependsOn("getRouterHost")

    val pg_router_host = if (project.extensions.extraProperties.has("pg_router_host")) {
        project.extensions.extraProperties["pg_router_host"] as String
    } else {
        "unknown"
    }
    args(
            "destroy",
            "-auto-approve",
            "-lock=false",
            "-var=pg_router_host=$pg_router_host",
            "-var=project_id=$(gcloud config get-value project)",
            "-var-file=./common.tfvars"
    )
}

tasks.register("getRouterHost") {
    group = "backend-deploy"
    doLast {
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
        val pgRouterHost = result.toString().trim().replace("'", "")
        project.extensions.extraProperties["pg_router_host"] = pgRouterHost
    }
}

tasks.register("indexcreate") {
    group = "backend-deploy"
    dependsOn("getRouterHost")

    doLast {

        val indexpath = "../backend/internal/storage/index.yaml"
        exec {
            executable("gcloud")
            args("datastore", "indexes", "create", indexpath)
        }
    }
}

tasks.register("firebaseProjectCreate") {
    group = "frontend-deploy"
    dependsOn("prepareFirebasercConfig")

    doLast {
        val result = ByteArrayOutputStream()
        val projectId = project.property("project_id") as String
        exec {
            executable("firebase")
            args("projects:list")
            standardOutput = result
        }
        val output = result.toString().trim()
        if (output.contains(projectId)) {
            println("Firebase is already added to project $projectId.")
        } else {
            exec {
                executable("firebase")
                args("projects:addfirebase", projectId)
            }.assertNormalExitValue()
            println("Firebase has been added to project $projectId.")
        }
    }
}

tasks.register("firebaseHostingCreate") {
    group = "frontend-deploy"
    dependsOn("firebaseWebAppCreate")

    doLast {
        val projectId = project.property("project_id") as String
        val webapp_id = project.property("webapp_id") as String
        val listSitesResult = ByteArrayOutputStream()
        exec {
            executable("firebase")
            args("hosting:sites:list", "--project", projectId)
            workingDir("../frontend")
            standardOutput = listSitesResult
        }

        println(listSitesResult)
        val output = listSitesResult.toString()
        val regex = "\\b$webapp_id\\b".toRegex()
        if (regex.containsMatchIn(output)) {
            println("Firebase is already added to project $projectId.")
        } else {
            exec {
                executable("firebase")
                args("hosting:sites:create", webapp_id)
                workingDir("../frontend")
            }.assertNormalExitValue()
            println("Firebase hosting site has been added to project $projectId.")
        }

        exec {
            executable("firebase")
            args("target:apply", "hosting", webapp_id , webapp_id)
            workingDir("../frontend")

        }.assertNormalExitValue()

        val file = project.file("../frontend/firebase.json")
        val content = file.readText()

        val oldContent = """"public": "build/web","""
        val newContent = """"public": "build/web",
        "target": "$webapp_id","""
        val updatedContent = content.replace(oldContent, newContent)

        file.writeText(updatedContent)
    }
}

tasks.register("firebaseWebAppCreate") {
    group = "frontend-deploy"
    dependsOn("firebaseProjectCreate")

    doLast {
        val result = ByteArrayOutputStream()
        val projectId = project.property("project_id") as String
        val webappId = project.property("webapp_id") as String
        exec {
            executable("firebase")
            args("apps:list", "--project", projectId)
            standardOutput = result
        }
        println(result)
        val output = result.toString()
        if (output.contains(webappId)) {
            println("Webapp id $webappId is already created on the project: $projectId.")
            val regex = Regex("""$webappId[\W]+([\d:a-zA-Z]+)[\W]+WEB""")
            val firebaseAppId = regex.find(output)?.groupValues?.get(1)?.trim()
            println("Firebase app ID for existing Firebase Web App: $firebaseAppId")
            project.extensions.extraProperties["firebaseAppId"] = firebaseAppId
        } else {
            val result2 = ByteArrayOutputStream()
            exec {
                executable("firebase")
                args("apps:create", "WEB", webappId, "--project", projectId)
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
    group = "frontend-deploy"
    dependsOn("firebaseHostingCreate")

    doLast {
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
    group = "frontend-deploy"
    dependsOn("getSdkConfigWebApp")

    doLast {
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
    dependsOn("prepareFirebaseOptionsDart")
    doLast {
        exec {
            executable("flutter")
            args("pub", "get")
            workingDir("../../../playground/frontend/playground_components")
        }
    }
}

tasks.register("flutterPubRunPG") {
    dependsOn("flutterPubGetPG")

    doLast {
        exec {
            executable("flutter")
            args("pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
            workingDir("../../../playground/frontend/playground_components")
        }
    }
}

tasks.register("flutterPubGetTob") {
    dependsOn("flutterPubRunPG")
    dependsOn("getSdkConfigWebApp")

    doLast {
        exec {
            executable("flutter")
            args("pub", "get")
            workingDir("../frontend")
        }
    }
}

tasks.register("flutterPubRunTob") {
    dependsOn("flutterPubGetTob")

    doLast {
        exec {
            executable("flutter")
            args("pub", "run", "build_runner", "build", "--delete-conflicting-outputs")
            workingDir("../frontend")
        }
    }
}

tasks.register("flutterBuildWeb") {
    dependsOn("flutterPubRunTob")

    doLast {
        exec {
            executable("flutter")
            args("build", "web", "-v", "--release")
            workingDir("../frontend")
        }
    }
}

tasks.register("firebaseDeploy") {
    dependsOn("flutterBuildWeb")

    doLast {
        val projectId = project.property("project_id") as String
        val webapp_id = project.property("webapp_id") as String
        exec {
            commandLine("firebase", "deploy", "--only",  "hosting:$webapp_id", "--project", projectId)
            workingDir("../frontend")
        }
    }
}

tasks.register("prepareConfig") {
    group = "frontend-deploy"

    doLast {
        val region = project.property("region") as String
        val projectId = project.property("project_id") as String
        val environment = project.property("project_environment") as String
        val dnsName = project.property("dns-name") as String
        val configFileName = "config.dart"
        val modulePath = project(":learning:tour-of-beam:frontend").projectDir.absolutePath
        val file = File("$modulePath/lib/$configFileName")

        file.writeText(
            """
const _cloudFunctionsProjectRegion = '$region';
const _cloudFunctionsProjectId = '$projectId';
const cloudFunctionsBaseUrl = 'https://'
    '$region-$projectId'
    '.cloudfunctions.net/${environment}_';

"""
        )
    }
}

tasks.register("prepareFirebasercConfig") {
    group = "frontend-deploy"
    dependsOn("prepareConfig")

    doLast {
        val projectId = project.property("project_id") as String
        val configFileName = ".firebaserc"
        val modulePath = project(":learning:tour-of-beam:frontend").projectDir.absolutePath
        val file = File("$modulePath/$configFileName")

        file.writeText(
            """
{
  "projects": {
    "default": "$projectId"
  }
}
"""
        )
    }
}

tasks.register("uploadLearningMaterials") {
    group = "backend-deploy"
    dependsOn("terraformApplyBackend")

    doLast {
        val projectId = project.property("project_id") as String
        exec {
            commandLine("go", "run", "cmd/ci_cd/ci_cd.go")
            environment("DATASTORE_PROJECT_ID", projectId)
            environment("GOOGLE_PROJECT_ID", projectId)
            environment("TOB_LEARNING_ROOT", "../learning-content/")
            workingDir("../backend")
        }
    }
}

/* Tour of Beam backend init */
tasks.register("InitBackend") {
    group = "backend-deploy"

    dependsOn("getRouterHost")
    dependsOn("indexcreate")
    dependsOn("terraformInit")
    dependsOn("terraformApplyBackend")
    dependsOn("uploadLearningMaterials")
}

tasks.register("DestroyBackend") {
    group = "backend-destroy"

    dependsOn("getRouterHost")
    dependsOn("terraformDestroy")
}

tasks.register("InitFrontend") {
    group = "frontend-deploy"

    dependsOn("prepareConfig")
    dependsOn("prepareFirebasercConfig")
    dependsOn("firebaseProjectCreate")
    dependsOn("firebaseWebAppCreate")
    dependsOn("firebaseHostingCreate")
    dependsOn("getSdkConfigWebApp")
    dependsOn("prepareFirebaseOptionsDart")
    dependsOn("flutterPubGetPG")
    dependsOn("flutterPubRunPG")
    dependsOn("flutterPubGetTob")
    dependsOn("flutterPubRunTob")
    dependsOn("flutterBuildWeb")
    dependsOn("firebaseDeploy")
}
