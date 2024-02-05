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

const exec = require("@actions/exec");
const github = require("./shared/githubUtils");
const commentStrings = require("./shared/commentStrings");
const {
  REPO_OWNER,
  REPO,
  BOT_NAME,
  PATH_TO_CONFIG_FILE,
} = require("./shared/constants");
const { ReviewerConfig } = require("./shared/reviewerConfig");

async function getPullsFromLastThreeMonths(): Promise<any[]> {
  const cutoffDate = new Date();
  cutoffDate.setMonth(cutoffDate.getMonth() - 3);
  console.log(`Getting PRs newer than ${cutoffDate}`);
  const githubClient = github.getGitHubClient();
  let result = await githubClient.rest.pulls.list({
    owner: REPO_OWNER,
    repo: REPO,
    state: "closed",
    per_page: 100, // max allowed
  });
  let page = 2;
  let retries = 0;
  let pulls = result.data;
  while (
    result.data.length > 0 &&
    new Date(result.data[result.data.length - 1].created_at) > cutoffDate
  ) {
    if (retries === 0) {
      console.log(`Getting PRs, page: ${page}`);
      console.log(
        `Current oldest PR = ${new Date(
          result.data[result.data.length - 1].created_at
        )}`
      );
    }
    try {
      result = await githubClient.rest.pulls.list({
        owner: REPO_OWNER,
        repo: REPO,
        state: "closed",
        per_page: 100, // max allowed
        page: page,
      });
      pulls = pulls.concat(result.data);
      page++;
      retries = 0;
    } catch (err) {
      if (retries >= 3) {
        throw err;
      }
      retries++;
    }
  }
  return pulls;
}

interface pullActivity {
  reviews: number;
  pullsAuthored: number;
}

interface reviewerActivity {
  reviewers: { [reviewer: string]: pullActivity };
}

async function getReviewersForPull(pull: any): Promise<string[]> {
  let reviewers = new Set<string>();
  const githubClient = github.getGitHubClient();
  let comments = (
    await githubClient.rest.issues.listComments({
      owner: REPO_OWNER,
      repo: REPO,
      issue_number: pull.number,
    })
  ).data;
  const reviewComments = (
    await githubClient.rest.pulls.listReviewComments({
      owner: REPO_OWNER,
      repo: REPO,
      pull_number: pull.number,
    })
  ).data;
  comments = comments.concat(reviewComments);

  for (const comment of comments) {
    if (
      comment.user &&
      comment.user.login &&
      comment.user.login !== pull.user.login &&
      comment.user.login !== BOT_NAME
    ) {
      reviewers.add(comment.user.login.toLowerCase());
    }
  }

  return [...reviewers];
}

function addReviewerActivity(
  reviewerActivity: reviewerActivity,
  reviewers: string[],
  author: string
): reviewerActivity {
  if (!reviewerActivity) {
    reviewerActivity = {
      reviewers: {},
    };
  }

  if (author in reviewerActivity.reviewers) {
    reviewerActivity.reviewers[author].pullsAuthored++;
  } else {
    reviewerActivity.reviewers[author] = {
      reviews: 0,
      pullsAuthored: 1,
    };
  }

  for (const reviewer of reviewers) {
    if (reviewer !== author) {
      if (reviewer in reviewerActivity.reviewers) {
        reviewerActivity.reviewers[reviewer].reviews++;
      } else {
        reviewerActivity.reviewers[reviewer] = {
          reviews: 1,
          pullsAuthored: 0,
        };
      }
    }
  }

  return reviewerActivity;
}

async function getReviewerActivityByLabel(
  pulls: any[]
): Promise<{ [label: string]: reviewerActivity }> {
  let reviewerActivityByLabel: { [label: string]: reviewerActivity } = {};
  for (const pull of pulls) {
    console.log(`Processing PR ${pull.number}`);
    const author = pull.user.login.toLowerCase();
    if (author !== BOT_NAME) {
      const reviewers = await getReviewersForPull(pull);
      const labels = pull.labels;
      for (const label of labels) {
        const labelName = label.name.toLowerCase();
        reviewerActivityByLabel[labelName] = addReviewerActivity(
          reviewerActivityByLabel[labelName],
          reviewers,
          author
        );
      }
    }
  }

  return reviewerActivityByLabel;
}

interface configUpdates {
  reviewersAddedForLabels: { [reviewer: string]: string[] };
  reviewersRemovedForLabels: { [reviewer: string]: string[] };
}

function reviewerIsBot(reviewer: string): boolean {
  if (
    ["codecov", "github-actions"].find(
      (bot) => reviewer.toLowerCase().indexOf(bot) != -1
    )
  ) {
    return true;
  }
  return false;
}

function updateReviewerConfig(
  reviewerActivityByLabel: { [label: string]: reviewerActivity },
  reviewerConfig: typeof ReviewerConfig
): configUpdates {
  let updates: configUpdates = {
    reviewersAddedForLabels: {},
    reviewersRemovedForLabels: {},
  };
  const currentReviewersForLabels = reviewerConfig.getReviewersForAllLabels();
  for (const label of Object.keys(currentReviewersForLabels)) {
    // Remove any reviewers with no reviews or pulls created
    let reviewers = currentReviewersForLabels[label];
    let updatedReviewers: string[] = [];
    const exclusionList = reviewerConfig.getExclusionListForLabel(label);
    for (const reviewer of reviewers) {
      if (reviewerActivityByLabel[label].reviewers[reviewer]) {
        updatedReviewers.push(reviewer);
      } else {
        if (reviewer in updates.reviewersRemovedForLabels) {
          updates.reviewersRemovedForLabels[reviewer].push(label);
        } else {
          updates.reviewersRemovedForLabels[reviewer] = [label];
        }
      }
    }

    // Add any reviewers who have at least 5 combined reviews + pulls authored
    for (const reviewer of Object.keys(
      reviewerActivityByLabel[label].reviewers
    )) {
      const reviewerContributions =
        reviewerActivityByLabel[label].reviewers[reviewer].reviews +
        reviewerActivityByLabel[label].reviewers[reviewer].pullsAuthored;

      if (
        reviewerContributions >= 5 &&
        updatedReviewers.indexOf(reviewer) < 0 &&
        exclusionList.indexOf(reviewer) < 0 &&
        !reviewerIsBot(reviewer)
      ) {
        updatedReviewers.push(reviewer);
        if (reviewer in updates.reviewersAddedForLabels) {
          updates.reviewersAddedForLabels[reviewer].push(label);
        } else {
          updates.reviewersAddedForLabels[reviewer] = [label];
        }
      }
    }

    console.log(
      `Updated reviewers for label ${label}: ${updatedReviewers.join(",")}`
    );

    reviewerConfig.updateReviewerForLabel(label, updatedReviewers);
  }

  return updates;
}

async function openPull(updates: configUpdates) {
  const curDate = new Date();
  const branch = `pr-bot-${
    curDate.getMonth() + 1
  }-${curDate.getDay()}-${curDate.getFullYear()}-${curDate.getHours()}-${curDate.getMinutes()}-${curDate.getSeconds()}`;
  await exec.exec(`git config user.email ${BOT_NAME}@github.com`);
  await exec.exec("git config pull.rebase false");
  await exec.exec(`git checkout -b ${branch}`);
  await exec.exec(`git add ${PATH_TO_CONFIG_FILE}`);
  await exec.exec(
    `git commit -m "Updating reviewer config based on historical trends"`
  );
  await exec.exec(`git push origin ${branch}`);

  const prBody = commentStrings.updateReviewerConfig(
    updates.reviewersAddedForLabels,
    updates.reviewersRemovedForLabels
  );
  await github.getGitHubClient().rest.pulls.create({
    owner: REPO_OWNER,
    repo: REPO,
    head: branch,
    base: "master",
    title: "Update reviewer config to reviewers with activity in repo areas",
    body: prBody,
    maintainer_can_modify: true,
  });
}

async function updateReviewers() {
  const pulls = await getPullsFromLastThreeMonths();
  console.log("Got all PRs, moving to the processing stage");
  const reviewerActivityByLabel = await getReviewerActivityByLabel(pulls);
  console.log("Processed all PRs to determine reviewer activity");
  let reviewerConfig = new ReviewerConfig(PATH_TO_CONFIG_FILE);
  const updates = updateReviewerConfig(reviewerActivityByLabel, reviewerConfig);
  if (
    Object.keys(updates.reviewersAddedForLabels).length > 0 ||
    Object.keys(updates.reviewersRemovedForLabels).length > 0
  ) {
    console.log(`Suggested updates to ${PATH_TO_CONFIG_FILE}`);
    await openPull(updates);
  } else {
    console.log("No updates to suggest");
  }
}

updateReviewers();

export {};
