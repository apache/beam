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

const { getGitHubClient } = require("./githubUtils");

export interface CheckStatus {
  completed: boolean;
  succeeded: boolean;
}

// Returns the status of the most recent checks runs -
export async function getChecksStatus(
  owner: string,
  repo: string,
  checkSha: string
): Promise<CheckStatus> {
  let checkStatus: CheckStatus = {
    completed: true,
    succeeded: true,
  };
  const mostRecentChecks = await getMostRecentChecks(owner, repo, checkSha);
  for (let i = 0; i < mostRecentChecks.length; i++) {
    if (mostRecentChecks[i].status != "completed") {
      checkStatus.completed = false;
    }
    if (
      mostRecentChecks[i].conclusion != "success" &&
      mostRecentChecks[i].conclusion != "skipped" &&
      mostRecentChecks[i].conclusion != "neutral"
    ) {
      checkStatus.succeeded = false;
    }
  }

  return checkStatus;
}

async function getMostRecentChecks(
  owner: string,
  repo: string,
  checkSha: string
): Promise<any> {
  let mostRecentChecks: any[] = [];
  const checksByName = await getChecksByName(owner, repo, checkSha);

  const checkNames = Object.keys(checksByName);
  for (let i = 0; i < checkNames.length; i++) {
    let checks = checksByName[checkNames[i]];
    let mostRecent = checks.sort((a, b) =>
      a.completionTime > b.completionTime ? 1 : -1
    )[0];
    mostRecentChecks.push(mostRecent);
  }

  return mostRecentChecks;
}

async function getChecksByName(
  owner: string,
  repo: string,
  checkSha: string
): Promise<any> {
  const githubClient = getGitHubClient();
  const allChecks = (
    await githubClient.rest.checks.listForRef({
      owner: owner,
      repo: repo,
      ref: checkSha,
    })
  ).data.check_runs;
  let checksByName = {};
  allChecks.forEach((checkRun) => {
    if (!shouldExcludeCheck(checkRun)) {
      let name = checkRun.name;
      let check = {
        status: checkRun.status,
        conclusion: checkRun.conclusion,
        completionTime: checkRun.completed_at,
      };
      if (!checksByName[name]) {
        checksByName[name] = [check];
      } else {
        checksByName[name].push(check);
      }
    }
  });

  return checksByName;
}

// Returns checks we should exclude because they are flaky or not always predictive of pr mergability.
// Currently just excludes codecov.
function shouldExcludeCheck(check): boolean {
  if (check.name.toLowerCase().indexOf("codecov") != -1) {
    return true;
  }
  return false;
}
