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

const { Octokit } = require("@octokit/rest");
const nodemailer = require('nodemailer');

const ONE_HOUR = 60 * 60 * 1000;

function sendReport(title, report) {
      
    nodemailer.createTransport({
        service: process.env['ISSUE_REPORT_SENDER_EMAIL_SERVICE'], // e.g. "gmail"
        auth: {
            user: process.env['ISSUE_REPORT_SENDER_EMAIL_ADDRESS'],
            pass: process.env['ISSUE_REPORT_SENDER_EMAIL_PASSWORD']
        }
    }).sendMail({
        from: process.env['ISSUE_REPORT_SENDER_EMAIL_ADDRESS'],
        to: process.env['ISSUE_REPORT_RECIPIENT_EMAIL_ADDRESS'],
        subject: title,
        text: report
    }, function(error, info){
        if (error) {
            throw new Error(`Failed to send email with error: ${error}`);
        } else {
            console.log('Email sent: ' + info.response);
        }
    });
}

function formatIssues(header, issues) {
    let report = header + "\n\n"
    for (const issue of issues) {
        report += `${issue.html_url} ${issue.title}\n`;
    }
    report += "\n\n"
    
    return report;
}

function getDateAge(updated_at) {
    return (new Date() - new Date(updated_at))
}

async function generateReport() {
    const octokit = new Octokit();

    let shouldSend = false;
    let report = `This is your daily summary of Beam's current high priority issues that may need attention.

    See https://beam.apache.org/contribute/issue-priorities for the meaning and expectations around issue priorities.

`;

    const p0Issues = await octokit.paginate(octokit.rest.issues.listForRepo, {
        owner: 'apache',
        repo: 'beam',
        labels: 'P0'
    });
    const p1Issues = await octokit.paginate(octokit.rest.issues.listForRepo, {
        owner: 'apache',
        repo: 'beam',
        labels: 'P1'
    });
    const unassignedP0Issues = p0Issues.filter(i => i.assignees.length == 0);
    const oldP0Issues = p0Issues.filter(i => i.assignees.length > 0 && getDateAge(i.updated_at) > 36*ONE_HOUR)
    const unassignedP1Issues = p1Issues.filter(i => i.assignees.length == 0);;
    const oldP1Issues = p1Issues.filter(i => i.assignees.length > 0 && getDateAge(i.updated_at) > 7*24*ONE_HOUR)
    if (unassignedP0Issues.length > 0) {
        shouldSend = true;
        report += formatIssues("Unassigned P0 Issues:", unassignedP0Issues);
    }
    if (oldP0Issues.length > 0) {
        shouldSend = true;
        report += formatIssues("P0 Issues with no update in the last 36 hours:", oldP0Issues);
    }
    if (unassignedP1Issues.length > 0) {
        shouldSend = true;
        report += formatIssues("Unassigned P1 Issues:", unassignedP1Issues);
    }
    if (oldP1Issues.length > 0) {
        shouldSend = true;
        report += formatIssues("P1 Issues with no update in the last week:", oldP1Issues);
    }

    if (shouldSend) {
        const totalCount = unassignedP1Issues.length + oldP1Issues.length + unassignedP0Issues.length + oldP0Issues.length
        sendReport(`Beam High Priority Issue Report (${totalCount})`, report);
    }
}

function validateEnvSet(envVar) {
    if (!process.env[envVar]) {
        throw new Error(`${envVar} environment variable not set.`)
    }
}

validateEnvSet('ISSUE_REPORT_SENDER_EMAIL_SERVICE')
validateEnvSet('ISSUE_REPORT_SENDER_EMAIL_ADDRESS')
validateEnvSet('ISSUE_REPORT_SENDER_EMAIL_PASSWORD')
validateEnvSet('ISSUE_REPORT_RECIPIENT_EMAIL_ADDRESS')

generateReport();
