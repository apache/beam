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
            "-var=project_id=$project_id",
            "-var=environment=$environment"
        )
    }
    /* refresh Infrastucture for remote state */
    register<TerraformTask>("terraformRef") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        args(
            "refresh",
            "-var=project_id=$project_id",
            "-var=environment=$environment"
        )
    }

    /* deploy all App */
    register<TerraformTask>("terraformApplyApp") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        var docker_tag = if (project.hasProperty("docker-tag")) { project.property("docker-tag") as String } else {environment}
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.applications",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag"
        )
    }

    /* deploy  App - Only all services for  backend */
    register<TerraformTask>("terraformApplyAppBack") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        var docker_tag = if (project.hasProperty("docker-tag")) { project.property("docker-tag") as String } else {environment}
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.applications.module.backend",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag"
        )
    }

    /* deploy  App - Only services for frontend */
    register<TerraformTask>("terraformApplyAppFront") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        var docker_tag = if (project.hasProperty("docker-tag")) { project.property("docker-tag") as String } else {environment}
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.applications.module.frontend",
            "-var=project_id=$project_id",
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
            "-var=environment=$environment"
        )
    }

    /* build All */
    register<TerraformTask>("terraformApply") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        var docker_tag = if (project.hasProperty("docker-tag")) { project.property("docker-tag") as String } else {environment}
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-var=project_id=$project_id",
            "-var=environment=$environment",
            "-var=docker_image_tag=$docker_tag"
        )
    }

    register<TerraformTask>("terraformDestroy") {
        var project_id = project.property("project_id") as String?
        var environment = project.property("project_environment") as String?
        args("destroy", "-auto-approve", "-lock=false", "-var=project_id=$project_id", "-var=environment=$environment")
    }
}

