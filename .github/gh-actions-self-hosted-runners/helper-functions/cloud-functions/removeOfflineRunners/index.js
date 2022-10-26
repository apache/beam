/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Unused offline self-hosted runners remains in the runners list
// unless it is explicitly removed, this function will periodically
// clean the list to only have active runners in the repo.

import functions from '@google-cloud/functions-framework';
import { Octokit } from "octokit";
import { createAppAuth } from "@octokit/auth-app";
import { REQUIRED_ENV_VARS } from "../shared/constants" ;

function validateEnvSet(envVars) {
    envVars.forEach(envVar => {
        if (!process.env[envVar]) {
            throw new Error(`${envVar} environment variable not set.`)
        }
    });
}

async function removeOfflineRunners() {
    try {
        //Set your GH App values as environment variables
        let authOptions = {
            appId: process.env.APP_ID,
            privateKey: process.env.PEM_KEY,
            clientId: process.env.CLIENT_ID,
            clientSecret: process.env.CLIENT_SECRET,
            installationId: process.env.APP_INSTALLATION_ID
        }
        const octokit = new Octokit({
            authStrategy: createAppAuth,
            auth: authOptions
        });

        const runners = await octokit.paginate("GET /orgs/${process.env.ORG}/actions/runners", {
            org: process.env.ORG
            },
        )

        //Filtering BEAM runners
        let beamRunners = runners.filter(runner => {
            return runner.labels.find(label => label.name == "beam")
        });

        //Getting offline runners only
        let offlineRunners = beamRunners.filter(runner => {
            return runner.status == "offline";
        })

        //Deleting each offline runner in the list
        for (let runner of offlineRunners) {
            await octokit.request(`DELETE /orgs/${process.env.ORG}/actions/runners/${runner.id}`, {});
        }
        return offlineRunners
    } catch (error) {
        console.error(error);
    }
}

functions.http('removeOfflineRunners', (req, res) => {
    validateEnvSet(REQUIRED_ENV_VARS)
    removeOfflineRunners().then((status) => {
        res.status(200).send(status);
    });
});
