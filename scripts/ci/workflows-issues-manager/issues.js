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

const { ISSUES_MANAGER_TAG, ISSUES_MANAGER_LABEL } = require("./constants.js");

const getRepoIssues = async ({ github, context }) => {
  let issues = [];

  for await (const response of github.paginate.iterator(github.rest.issues.listForRepo, {
    owner: context.repo.owner,
    repo: context.repo.repo,
    labels: ISSUES_MANAGER_LABEL,
    per_page: 100,
  })) {
    issues = issues.concat(response.data);
  }

  return issues;
};

const createIssue = async ({ github, context }, workflow) => {
  //Issue needs to have the following format so it can be found by the bot
  const fileName = workflow.path.split("/").pop();
  const issueTitle = `[${ISSUES_MANAGER_TAG}] [${workflow.id}]: ${workflow.name}`;
  const issueBody = `
  ### What happened?

  **${workflow.name}** is faling, please take a look:

  - Workflow URL: ${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/workflows/${fileName}
  - File URL: ${workflow.html_url}

  ### Issue Priority

  Priority: 1

  ### Issue Component

  Component: testing
  `;

  // TODO: add labels from default labels
  const { data } = await github.rest.issues.create({
    owner: context.repo.owner,
    repo: context.repo.repo,
    title: issueTitle,
    body: issueBody,
  });
  return data.html_url;
};

const closeIssue = async ({ github, context }, issue) => {
  const { data } = await github.rest.issues.update({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: issue.number,
    state: "closed",
  });

  return data.html_url;
};

module.exports = { getRepoIssues, createIssue, closeIssue };
