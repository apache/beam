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

// This function will return the number of online and offline runners for
// each OS (Windows, linux), an additional Github actions workflow will run
// to request this Cloud Function and send alerts based on the status.

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

async function monitorRunnerStatus() {
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

        //Dividing status for each runner OS
        let osList = ["Linux", "Windows"];
        let status = {}
        for (let os of osList) {
            let osRunners = beamRunners.filter(runner => {
                return runner.labels.find(label => label.name == os)
            });
            let onlineRunners = osRunners.filter(runner => {
                return runner.status == "online";
            });
            status[os] = {
                "totalRunners": osRunners.length,
                "onlineRunners": onlineRunners.length,
                "offlineRunners": osRunners.length - onlineRunners.length
            }
        }
        return status;
    } catch (error) {
        console.error(error);
    }
}

functions.http('monitorRunnerStatus', (req, res) => {
    validateEnvSet(REQUIRED_ENV_VARS)
    monitorRunnerStatus().then((status) => {
        res.status(200).send(status);
    });
});
