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

// This function provides registration tokens for windows and linux self-hosted
// runners, a service account with the appropriated permissions should be used to
// invoke the Cloud Function.

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
async function getRunnerToken() {
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
        let access = await octokit.request(`POST /app/installations/${process.env.APP_INSTALLATION_ID}}/access_tokens`, {
            repositories: [
                'beam'
            ],
            permissions: {
                organization_self_hosted_runners: "write",
            }
        });
        //In order to access the registration token endpoint, an additional Auth token must be used
        let authToken = access.data.token;
        let auth = ` token ${authToken}`;
        let registrationToken = await octokit.request(`POST https://api.github.com/orgs/${process.env.ORG}/actions/runners/registration-token`, {
            headers: {
                authorization: auth,
            },
        });
        return registrationToken.data;
    } catch (error) {
        console.error(error);
    }
}

functions.http('generateToken', (req, res) => {
    validateEnvSet(REQUIRED_ENV_VARS)
    getRunnerToken().then((registrationToken) => {
        res.status(200).send(registrationToken);
    });
});
