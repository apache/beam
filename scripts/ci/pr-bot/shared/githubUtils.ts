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
const { REPO_OWNER, REPO } = require("./constants");

export function getGitHubClient() {
  let githubToken = process.env["GITHUB_TOKEN"];
  if (!githubToken) {
    throw new Error(
      "No github token provided - process.env['GITHUB_TOKEN'] must be set."
    );
  }
  return new Octokit({ auth: githubToken });
}

export async function addPrComment(pullNumber: number, body: string) {
  const githubClient = getGitHubClient();
  await githubClient.rest.issues.createComment({
    owner: REPO_OWNER,
    repo: REPO,
    issue_number: pullNumber,
    body: body,
  });
}

export async function nextActionReviewers(
  pullNumber: number,
  existingLabels: any[]
) {
  const client = getGitHubClient();
  let newLabels = removeNextActionLabel(existingLabels);
  newLabels.push("Next Action: Reviewers");
  await client.rest.issues.setLabels({
    owner: REPO_OWNER,
    repo: REPO,
    issue_number: pullNumber,
    labels: newLabels,
  });
}

export async function nextActionAuthor(
  pullNumber: number,
  existingLabels: any[]
) {
  const client = getGitHubClient();
  let newLabels = removeNextActionLabel(existingLabels);
  newLabels.push("Next Action: Author");
  await client.rest.issues.setLabels({
    owner: REPO_OWNER,
    repo: REPO,
    issue_number: pullNumber,
    labels: newLabels,
  });
}

export async function checkIfCommitter(username: string): Promise<boolean> {
  const client = getGitHubClient();
  const permissions = (
    await client.rest.repos.getCollaboratorPermissionLevel({
      owner: REPO_OWNER,
      repo: REPO,
      username: username,
    })
  ).data.permission;

  return permissions == "write" || permissions == "admin";
}

function removeNextActionLabel(existingLabels: any[]): string[] {
  let newLabels: string[] = [];
  existingLabels.forEach((label) => {
    if (
      label.name != "Next Action: Reviewers" &&
      label.name != "Next Action: Author"
    ) {
      newLabels.push(label.name);
    }
  });
  return newLabels;
}
