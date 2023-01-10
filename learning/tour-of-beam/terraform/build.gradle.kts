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
    /* Terraform initialization task */
    register<TerraformTask>("terraformInit") {
        var state_bucket = "unknown"
        if (project.hasProperty("state_bucket")) {
            state_bucket = project.property("state_bucket") as String
        }
        args(
            "init", "-migrate-state",
            "-backend-config=bucket=$state_bucket"
        )
    }

    /* Terraform Aplly task */
    register<TerraformTask>("terraformApply") {
        args(
                "apply",
                "-auto-approve",
                "-lock=false",
                "-var='project_id=$(gcloud config get-value project)'"
        )
    }
}

/* initialization infrastructure */
tasks.register("InitInfrastructure") {
description = "initialization infrastructure"
val init = tasks.getByName("terraformInit")
val apply = tasks.getByName("terraformApply")
dependsOn(init)
dependsOn(apply)
apply.mustRunAfter(init)
}
