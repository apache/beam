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

    register<TerraformTask>("terraformInit") {
        // exec args can be passed by commandline, for example
        var project_id = System.getenv("PLAYGROUND_PROJECT_ID")
        var environment = System.getenv("PLAYGROUND_ENVIRONMENT")
        args(
            "init", "-upgrade",
            "-var=project_id=$project_id",
            "-var=environment=$environment"
        )
    }

    register<TerraformTask>("terraformApplyApp") {
        var project_id = System.getenv("PLAYGROUND_PROJECT_ID")
        var environment = System.getenv("PLAYGROUND_ENVIRONMENT")
        var docker_tag = System.getenv("PLAYGROUND_DOCKER_TAG")
        )
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

    register<TerraformTask>("terraformApplyAppBack") {
        var project_id = System.getenv("PLAYGROUND_PROJECT_ID")
        var environment = System.getenv("PLAYGROUND_ENVIRONMENT")
        var docker_tag = System.getenv("PLAYGROUND_DOCKER_TAG")
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
    register<TerraformTask>("terraformApplyAppFront") {
        var project_id = System.getenv("PLAYGROUND_PROJECT_ID")
        var environment = System.getenv("PLAYGROUND_ENVIRONMENT")
        var docker_tag = System.getenv("PLAYGROUND_DOCKER_TAG")
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

    register<TerraformTask>("terraformApplyInf") {
        var project_id = System.getenv("PLAYGROUND_PROJECT_ID")
        var environment = System.getenv("PLAYGROUND_ENVIRONMENT")
        args(
            "apply",
            "-auto-approve",
            "-lock=false",
            "-target=module.infrastructure",
            "-var=project_id=$project_id",
            "-var=environment=$environment"
        )
    }

    register<TerraformTask>("terraformApply") {
        var project_id = System.getenv("PLAYGROUND_PROJECT_ID")
        var environment = System.getenv("PLAYGROUND_ENVIRONMENT")
        var docker_tag = System.getenv("PLAYGROUND_DOCKER_TAG")
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
        var project_id = System.getenv("PLAYGROUND_PROJECT_ID")
        var environment = System.getenv("PLAYGROUND_ENVIRONMENT")
        args("destroy", "-auto-approve", "-lock=false", "-var=project_id=$project_id", "-var=environment=$environment")
    }


}
