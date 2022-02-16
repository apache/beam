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
    /* init Infrastucture for migrate */
    register<TerraformTask>("terraformInit") {
        // exec args can be passed by commandline, for example
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        args(
            "init", "-migrate-state",
            "-backend-config=./environment/$environment/state.tfbackend",
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=project_id=$project_id",
            "-var=environment=$environment"
        )
    }
    /* refresh Infrastucture for remote state */
    register<TerraformTask>("terraformRef") {
        mustRunAfter(":playground:terraform:terraformInit")
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        args(
            "refresh",
            "-lock=false",
            "-var=project_id=$project_id",
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=environment=$environment"
        )
    }

    /* deploy all App */
    register<TerraformTask>("terraformApplyApp") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
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
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag"
        )
    }

    /* deploy  App - Only all services for  backend */
    register<TerraformTask>("terraformApplyAppBack") {
        println("Deploy Back")
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        var docker_tag = if (project.hasProperty("docker-tag")) {
            project.property("docker-tag") as String
        } else {
            environment
        }
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.applications.module.backend",
            "-var=project_id=$project_id",
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag"
        )
    }

    /* deploy  App - Only services for frontend */
    register<TerraformTask>("terraformApplyAppFront") {
        println("Deploy Front")
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
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
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag"
        )
    }

    /* build only Infrastructurte */
    register<TerraformTask>("terraformApplyInf") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.infrastructure",
            "-var=project_id=$project_id",
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=environment=$environment"
        )
    }

    /* build All */
    register<TerraformTask>("terraformApply") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
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
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag"
        )
    }

    register<TerraformTask>("terraformDestroy") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        args(
            "destroy",
            "-auto-approve",
            "-lock=false",
            "-var=project_id=$project_id",
            "-var-file=./environment/$environment/terraform.tfvars",
            "-var=environment=$environment"
        )
    }
}


/* set Docker Registry to params from Inf */
task("setDockerRegistry") {
    //get Docker Registry
    dependsOn(":playground:terraform:terraformInit")
    dependsOn(":playground:terraform:terraformRef")
    try {
        var stdout = ByteArrayOutputStream()
        //set Docker Registry
        exec {
            commandLine = listOf("terraform", "output", "docker-repository-root")
            standardOutput = stdout
        }
        project.rootProject.extra["docker-repository-root"] = stdout.toString().trim().replace("\"", "")
    } catch (e: Exception) {
    }
}

task("setFrontConfig") {
//get Docker Registry
    dependsOn(":playground:terraform:terraformInit")
    dependsOn(":playground:terraform:terraformRef")
    try {
        var stdout = ByteArrayOutputStream()
//set GO - playgroundBackendGoRouteUrl
        exec {
            commandLine = listOf("terraform", "output", "go-server-url")
            standardOutput = stdout

        }
        project.rootProject.extra["playgroundBackendGoRouteUrl"] = stdout.toString().trim().replace("\"", "")
        println("GO app address:")
//set Java - playgroundBackendJavaRouteUrl

        exec {
            commandLine = listOf("terraform", "output", "java-server-url")
            standardOutput = stdout
        }
        project.rootProject.extra["playgroundBackendJavaRouteUrl"] = stdout.toString().trim().replace("\"", "")

        println("Java app address:")

//set Python - playgroundBackendPythonRouteUrl
        exec {
            commandLine = listOf("terraform", "output", "python-server-url")
            standardOutput = stdout
        }
        project.rootProject.extra["playgroundBackendPythonRouteUrl"] = stdout.toString().trim().replace("\"", "")
        println("Python app address:")
//set Router - playgroundBackendUrl
        exec {
            commandLine = listOf("terraform", "output", "router-server-url")
            standardOutput = stdout
        }
        project.rootProject.extra["playgroundBackendUrl"] = stdout.toString().trim().replace("\"", "")
        println("Router app address:")
//set Scio - playgroundBackendScioRouteUrl
        exec {
            commandLine = listOf("terraform", "output", "scio-server-url")
            standardOutput = stdout
        }
        project.rootProject.extra["playgroundBackendScioRouteUrl"] = stdout.toString().trim().replace("\"", "")
        println("Scio app address:")
    } catch (e: Exception) {
    }
}

task("pushBack") {
    dependsOn(":playground:backend:containers:go:dockerTagsPush")
    dependsOn(":playground:backend:containers:java:dockerTagsPush")
    dependsOn(":playground:backend:containers:python:dockerTagsPush")
    dependsOn(":playground:backend:containers:scio:dockerTagsPush")
    dependsOn(":playground:backend:containers:router:dockerTagsPush")
}

task("pushFront") {
    dependsOn(":playground:frontend:createConfig")
    dependsOn(":playground:frontend:dockerTagsPush")
}

/* build, push, deploy Frontend app */
task("deployFrontend") {

    val config = tasks.getByName("setFrontConfig")
    val push = tasks.getByName("pushFront")
    val deploy = tasks.getByName("terraformApplyAppFront")

    dependsOn(push)
    dependsOn(config)
    dependsOn(deploy)
    push.mustRunAfter(config)
    deploy.mustRunAfter(push)
}

/* build, push, deploy Backend app */
task("deployBackend") {

    val config = tasks.getByName("setDockerRegistry")
    val push = tasks.getByName("pushBack")
    val deploy = tasks.getByName("terraformApplyAppBack")

    dependsOn(push)
    dependsOn(config)
    dependsOn(deploy)
    push.mustRunAfter(config)
    deploy.mustRunAfter(push)
}