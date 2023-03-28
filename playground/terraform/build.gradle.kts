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

description = "Apache Beam :: Playground :: Deploy"
val licenseText = "################################################################################\n" +
        "#  Licensed to the Apache Software Foundation (ASF) under one\n" +
        "#  or more contributor license agreements.  See the NOTICE file\n" +
        "#  distributed with this work for additional information\n" +
        "#  regarding copyright ownership.  The ASF licenses this file\n" +
        "#  to you under the Apache License, Version 2.0 (the\n" +
        "#  \"License\"); you may not use this file except in compliance\n" +
        "#  with the License.  You may obtain a copy of the License at\n" +
        "#\n" +
        "#      http://www.apache.org/licenses/LICENSE-2.0\n" +
        "#\n" +
        "#  Unless required by applicable law or agreed to in writing, software\n" +
        "#  distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        "#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        "#  See the License for the specific language governing permissions and\n" +
        "# limitations under the License.\n" +
        "################################################################################"

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
        mustRunAfter(":playground:terraform:terraformInit")
        var project_id = "unknown"
        var environment = "unknown"
        var region = "unknown"
        if (project.hasProperty("project_id")) {
            project_id = project.property("project_id") as String
        }
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
            "refresh",
            "-lock=false",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            "-var=region=$region",
            if (file("./environment/$environment/terraform.tfvars").exists()) {
                "-var-file=./environment/$environment/terraform.tfvars"
            } else {
                "-no-color"
            }
        )
    }

    /* deploy all App */
    register<TerraformTask>("terraformApplyApp") {
        var project_id = "unknown"
        var environment = "unknown"
        if (project.hasProperty("project_id")) {
            project_id = project.property("project_id") as String
        }
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        var docker_tag = if (project.hasProperty("docker-tag")) {
            project.property("docker-tag") as String
        } else {
            environment
        }
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.applications",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag",
            if (file("./environment/$environment/terraform.tfvars").exists()) {
                "-var-file=./environment/$environment/terraform.tfvars"
            } else {
                "-no-color"
            }
        )
    }

    /* deploy  App - Only all services for  backend */
    register<TerraformTask>("terraformApplyAppBack") {
        var environment = "unknown"
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.applications.module.backend",
            "-var=environment=$environment",
            if (file("./environment/$environment/terraform.tfvars").exists()) {
                "-var-file=./environment/$environment/terraform.tfvars"
            } else {
                "-no-color"
            }
        )
    }

    /* deploy  App - Only services for frontend */
    register<TerraformTask>("terraformApplyAppFront") {
        var project_id = "unknown"
        var environment = "unknown"
        if (project.hasProperty("project_id")) {
            project_id = project.property("project_id") as String
        }
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        var docker_tag = if (project.hasProperty("docker-tag")) {
            project.property("docker-tag") as String
        } else {
            environment
        }
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.applications.module.frontend",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag",
            if (file("./environment/$environment/terraform.tfvars").exists()) {
                "-var-file=./environment/$environment/terraform.tfvars"
            } else {
                "-no-color"
            }
        )
    }

    /* build only Infrastructurte */
    register<TerraformTask>("terraformApplyInf") {
        var environment = "unknown"
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.infrastructure",
            "-var=environment=$environment",
            if (file("./environment/$environment/terraform.tfvars").exists()) {
                "-var-file=./environment/$environment/terraform.tfvars"
            } else {
                "-no-color"
            }
        )
    }

    /* build All */
    register<TerraformTask>("terraformApply") {
        var project_id = "unknown"
        var environment = "unknown"
        if (project.hasProperty("project_id")) {
            project_id = project.property("project_id") as String
        }
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        var docker_tag = if (project.hasProperty("docker-tag")) {
            project.property("docker-tag") as String
        } else {
            environment
        }
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag",
            if (file("./environment/$environment/terraform.tfvars").exists()) {
                "-var-file=./environment/$environment/terraform.tfvars"
            } else {
                "-no-color"
            }
        )
    }

    register<TerraformTask>("terraformDestroy") {
        var project_id = "unknown"
        var environment = "unknown"
        if (project.hasProperty("project_id")) {
            project_id = project.property("project_id") as String
        }
        if (project.hasProperty("project_environment")) {
            environment = project.property("project_environment") as String
        }
        args(
            "destroy",
            "-auto-approve",
            "-lock=false",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            if (file("./environment/$environment/terraform.tfvars").exists()) {
                "-var-file=./environment/$environment/terraform.tfvars"
            } else {
                "-no-color"
            }
        )
    }
}

tasks.register("readState") {
    group = "deploy"
    dependsOn(":playground:terraform:terraformInit")
    dependsOn(":playground:terraform:terraformRef")
}

tasks.register("pushBack") {
    group = "deploy"

    dependsOn(":playground:backend:containers:go:dockerTagsPush")
    dependsOn(":playground:backend:containers:java:dockerTagsPush")
    dependsOn(":playground:backend:containers:python:dockerTagsPush")
    dependsOn(":playground:backend:containers:scio:dockerTagsPush")
    dependsOn(":playground:backend:containers:router:dockerTagsPush")
}

tasks.register("pushFront") {
    group = "deploy"

    dependsOn(":playground:frontend:dockerTagsPush")
}


/* initialization infrastructure */
tasks.register("InitInfrastructure") {
    group = "deploy"
    description = "initialization infrastructure"
    val init = tasks.getByName("terraformInit")
    val apply = tasks.getByName("terraformApplyInf")
    dependsOn(init)
    dependsOn(apply)
    apply.mustRunAfter(init)
}

tasks.register("indexcreate") {
    group = "deploy"
    val indexpath = "../index.yaml"
    doLast {
        exec {
            executable("gcloud")
            args("app", "deploy", indexpath)
        }
    }
}

/* build, push, deploy Frontend app */
tasks.register("deployFrontend") {
    group = "deploy"
    description = "deploy Frontend app"
    val read = tasks.getByName("readState")
    val push = tasks.getByName("pushFront")
    val deploy = tasks.getByName("terraformApplyAppFront")
    dependsOn(read)
    Thread.sleep(10)
    push.mustRunAfter(read)
    deploy.mustRunAfter(push)
    dependsOn(push)
    dependsOn(deploy)
}

tasks.register<TerraformTask>("setPlaygroundStaticIpAddress") {
    group = "deploy"

    dependsOn("terraformInit")
    dependsOn("terraformRef")

    args("output", "playground_static_ip_address")
    standardOutput = ByteArrayOutputStream()

    doLast {
        project.rootProject.extra["playground_static_ip_address"] = standardOutput.toString().trim().replace("\"", "")
    }
}

tasks.register<TerraformTask>("setPlaygroundRedisIp") {
    group = "deploy"

    dependsOn("terraformInit")
    dependsOn("terraformRef")

    args("output", "playground_redis_ip")
    standardOutput = ByteArrayOutputStream()

    doLast {
        project.rootProject.extra["playground_redis_ip"] = standardOutput.toString().trim().replace("\"", "")
    }
}

tasks.register<TerraformTask>("setPlaygroundGkeProject") {
    group = "deploy"

    dependsOn("terraformInit")
    dependsOn("terraformRef")

    args("output", "playground_gke_project")
    standardOutput = ByteArrayOutputStream()

    doLast {
        project.rootProject.extra["playground_gke_project"] = standardOutput.toString().trim().replace("\"", "")
    }
}

tasks.register<TerraformTask>("setPlaygroundStaticIpAddressName") {
    group = "deploy"

    dependsOn("terraformInit")
    dependsOn("terraformRef")

    args("output", "playground_static_ip_address_name")
    standardOutput = ByteArrayOutputStream()

    doLast {
        project.rootProject.extra["playground_static_ip_address_name"] =
            standardOutput.toString().trim().replace("\"", "")
    }
}

tasks.register("takeConfig") {
    group = "deploy"

    dependsOn("setPlaygroundStaticIpAddress")
    dependsOn("setPlaygroundRedisIp")
    dependsOn("setPlaygroundGkeProject")
    dependsOn("setPlaygroundStaticIpAddressName")

    doLast {
        var d_tag = ""
        var dns_name = ""
        if (project.hasProperty("dns-name")) {
            dns_name = project.property("dns-name") as String
        }
        if (project.hasProperty("docker-tag")) {
            d_tag = project.property("docker-tag") as String
        }

        val configFileName = "values.yaml"
        val modulePath = project(":playground").projectDir.absolutePath
        val file = File("$modulePath/infrastructure/helm-playground/$configFileName")
        val lines = file.readLines()
        val endOfSlice = lines.indexOfFirst { it.contains("static_ip") }
        if (endOfSlice != -1) {
            val oldContent = lines.slice(0 until endOfSlice)
            val flagDelete = file.delete()
            if (!flagDelete) {
                throw kotlin.RuntimeException("Deleting file failed")
            }
            val sb = kotlin.text.StringBuilder()
            val lastLine = oldContent[oldContent.size - 1]
            oldContent.forEach {
                if (it == lastLine) {
                    sb.append(it)
                } else {
                    sb.appendLine(it)
                }
            }
            file.writeText(sb.toString())
        }

        val ipaddr = project.rootProject.extra["playground_static_ip_address"]
        val redis = project.rootProject.extra["playground_redis_ip"]
        val proj = project.rootProject.extra["playground_gke_project"]
        val registry = project.rootProject.extra["docker-repository-root"]
        val ipaddrname = project.rootProject.extra["playground_static_ip_address_name"]

        file.appendText(
            """
static_ip: ${ipaddr}
redis_ip: ${redis}:6379
project_id: ${proj}
registry: ${registry}
static_ip_name: ${ipaddrname}
tag: $d_tag
dns_name: ${dns_name}
    """
        )
    }
}

tasks.register("helmRelease") {
    group = "deploy"
    val modulePath = project(":playground").projectDir.absolutePath
    val hdir = File("$modulePath/infrastructure/helm-playground/")
    doLast {
        exec {
            executable("helm")
            args("install", "playground", "$hdir")
        }
    }
}
tasks.register("gkebackend") {
    group = "deploy"
    val initTask = tasks.getByName("terraformInit")
    val takeConfigTask = tasks.getByName("takeConfig")
    val pushBackTask = tasks.getByName("pushBack")
    val pushFrontTask = tasks.getByName("pushFront")
    val indexcreateTask = tasks.getByName("indexcreate")
    val helmTask = tasks.getByName("helmRelease")
    dependsOn(initTask)
    dependsOn(takeConfigTask)
    dependsOn(pushBackTask)
    dependsOn(pushFrontTask)
    dependsOn(indexcreateTask)
    dependsOn(helmTask)
    takeConfigTask.mustRunAfter(initTask)
    pushBackTask.mustRunAfter(takeConfigTask)
    pushFrontTask.mustRunAfter(pushBackTask)
    indexcreateTask.mustRunAfter(pushFrontTask)
    helmTask.mustRunAfter(indexcreateTask)
}

