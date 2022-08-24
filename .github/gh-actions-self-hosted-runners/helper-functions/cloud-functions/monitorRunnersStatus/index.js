//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
// 
//    http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.

//This function will return the number of online and offline runners for
// each OS (Windows, linux), an additional Github actions workflow will run
// to request this Cloud Function and send alerts based on the status.

import functions from '@google-cloud/functions-framework';
import { Octokit } from "octokit";
import { createAppAuth } from "@octokit/auth-app";

async function monitorRunnerStatus() {
    try {
        //Set your GH App values as environment variables
        let authOptions = {
            appId: process.env.APP_ID,
            privateKey: process.env.PEM_KEY,
            clientId: process.env.CLIENT_ID,
            clientSecret: process.env.CLIENT_NAME,
            installationId: process.env.APP_INSTALLATION_ID
        }
        const octokit = new Octokit({
            authStrategy: createAppAuth,
            auth: authOptions
        });
        let runners = await octokit.request(`GET /orgs/${process.env.ORG}/actions/runners`, {
            org: process.env.ORG,
            per_page: 100, // In order to avoid cropped results we are explicitly setting this option in combination with a daily cleanup
        });

        //Filtering BEAM runners
        let beamRunners = runners.data.runners.filter(runner => {
            for (let label of runner.labels) {
                if (label.name == "beam") {
                    return true;
                }
            }
            return false;
        });

        //Dividing status for each runner OS    
        let osList = ["Linux", "Windows"];
        let status = {}
        for (let os of osList) {
            let osRunners = beamRunners.filter(runner => {
                for (let label of runner.labels) {
                    if (label.name == os) {
                        return true;
                    }
                }
                return false;
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
    monitorRunnerStatus().then((status) => {
        res.status(200).send(status);
    });
});
