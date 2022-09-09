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

const nodemailer = require("nodemailer");
const axios = require('axios');


async function getRunnersStatus() {
    let status = await axios.post(process.env["ENDPOINT"], {}, {
        headers: {
            Accept: "application/json",
            Authorization: "bearer " + process.env["IDENTITY_TOKEN"]
        }
    });
    return status.data;
}

async function sendAlertEmail(status) {
    statusTables = {}
    //Creating status tables
    for (let OS of ["Linux", "Windows"]) {
        statusTables[OS] = `
        <h3> ${OS} </h3>
        <table style='border: 1px solid grey;'>
            <tr>
                <th>Total Runners</th>
                <th>Online Runners </th>
                <th>Offline Runners</th>
            </tr>
        
            <tr>
                <td>${status[OS].totalRunners}</td>
                <td>${status[OS].onlineRunners}</td>
                <td>${status[OS].offlineRunners}</td>
            </tr>
        </table>
        `
    }

    const htmlMsg = ` 
        <p>Here is the runners status per Operative System, please inspect GCP console for further details: </p> <br>
        ` + statusTables["Linux"] + "<br>" + statusTables["Windows"];

    nodemailer.createTransport({
        service: process.env['ISSUE_REPORT_SENDER_EMAIL_SERVICE'], 
        auth: {
            user: process.env['ISSUE_REPORT_SENDER_EMAIL_ADDRESS'],
            pass: process.env['ISSUE_REPORT_SENDER_EMAIL_PASSWORD']
        }
    }).sendMail({
        from: process.env['ISSUE_REPORT_SENDER_EMAIL_ADDRESS'],
        to: process.env['ISSUE_REPORT_RECIPIENT_EMAIL_ADDRESS'],
        subject: "Alert; self-hosted runners are not healthy",
        html: htmlMsg,
    }, function (error, info) {
        if (error) {
            throw new Error(`Failed to send email with error: ${error}`);
        } else {
            console.log('Email sent: ' + info.response);
        }
    });
}

async function monitorRunnersStatus() {
    const status = await getRunnersStatus().catch(console.error);
    console.log(status);
    if (status.Linux.onlineRunners == 0 || status.Windows.onlineRunners == 0) {
        sendAlertEmail(status);
    } else {
        return 0;
    }
}

function validateEnvSet(envVar) {
    if (!process.env[envVar]) {
        throw new Error(`${envVar} environment variable not set.`)
    }
}

validateEnvSet('ISSUE_REPORT_RECIPIENT_EMAIL_ADDRESS')
validateEnvSet('ISSUE_REPORT_SENDER_EMAIL_PASSWORD')
validateEnvSet('ISSUE_REPORT_SENDER_EMAIL_ADDRESS')
validateEnvSet('IDENTITY_TOKEN')
validateEnvSet('ISSUE_REPORT_SENDER_EMAIL_SERVICE')
validateEnvSet('ENDPOINT')

monitorRunnersStatus();
