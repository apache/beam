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

tasks {
    /* Terraform initialization task */
    register<TerraformTask>("terraformInit") {
        args(
                "init", "-migrate-state",
                "-backend-config=./environment/$environment/state.tfbackend"
        )
    }

    /* deploy all App */
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
}

    tasks.register("gkebackend") {
        group = "deploy"
        val initTask = tasks.getByName("terraformInit")
        val docRegTask = tasks.getByName("setDockerRegistry")
        val takeConfigTask = tasks.getByName("takeConfig")
        val pushBackTask = tasks.getByName("pushBack")
        val pushFrontTask = tasks.getByName("pushFront")
        val indexcreateTask = tasks.getByName("indexcreate")
        val helmTask = tasks.getByName("helmRelease")
        dependsOn(initTask)
        dependsOn(docRegTask)
        dependsOn(takeConfigTask)
        dependsOn(pushBackTask)
        dependsOn(pushFrontTask)
        dependsOn(indexcreateTask)
        dependsOn(helmTask)
        docRegTask.mustRunAfter(initTask)
        takeConfigTask.mustRunAfter(docRegTask)
        pushBackTask.mustRunAfter(takeConfigTask)
        pushFrontTask.mustRunAfter(pushBackTask)
        indexcreateTask.mustRunAfter(pushFrontTask)
        helmTask.mustRunAfter(indexcreateTask)
    }

