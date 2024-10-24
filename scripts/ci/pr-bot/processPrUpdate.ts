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

const github = require("@actions/github");
const commentStrings = require("./shared/commentStrings");
const { processCommand } = require("./shared/userCommand");
const {
  addPrComment,
  getGitHubClient,
  nextActionReviewers,
  getPullAuthorFromPayload,
  getPullNumberFromPayload,
} = require("./shared/githubUtils");
const { PersistentState } = require("./shared/persistentState");
const { ReviewerConfig } = require("./shared/reviewerConfig");
const {
  BOT_NAME,
  PATH_TO_CONFIG_FILE,
  REPO_OWNER,
  REPO,
  SLOW_REVIEW_LABEL,
  REVIEWERS_ACTION,
} = require("./shared/constants");

// Removes the slow label if the pr has been reviewed and returns an updated payload.
async function removeSlowReviewLabel(payload: any) {
  let labels = payload.issue?.labels || payload.pull_request?.labels;
  if (labels.some((label) => label.name.toLowerCase() == SLOW_REVIEW_LABEL)) {
    const pullNumber = payload.issue?.number || payload.pull_request?.number;
    labels = (
      await getGitHubClient().rest.issues.removeLabel({
        owner: REPO_OWNER,
        repo: REPO,
        issue_number: pullNumber,
        name: "slow-review",
      })
    ).data;
    if (payload.issues) {
      payload.issues.labels = labels;
    }
    if (payload.pull_request) {
      payload.pull_request.labels = labels;
    }
  }
}

async function areReviewersAssigned(
  pullNumber: number,
  stateClient: typeof PersistentState
): Promise<boolean> {
  const prState = await stateClient.getPrState(pullNumber);
  return Object.values(prState.reviewersAssignedForLabels).length > 0;
}

async function processPrComment(
  payload: any,
  stateClient: typeof PersistentState,
  reviewerConfig: typeof ReviewerConfig
) {
  const commentContents = payload.comment.body;
  const commentAuthor = payload.sender.login;
  const pullAuthor = getPullAuthorFromPayload(payload);
  console.log(commentContents);
  const processedCommand = await processCommand(
    payload,
    commentAuthor,
    commentContents,
    stateClient,
    reviewerConfig
  );

  // Check to see if notifications have been stopped before processing further.
  // Notifications can be stopped by an "R: reviewer" comment,
  // and then restarted by adding "assign set of reviewers" comment.
  if (
    (await stateClient.getPrState(getPullNumberFromPayload(payload)))
      .stopReviewerNotifications
  ) {
    console.log("Notifications have been paused for this pull - skipping");
    return;
  }

  // If there's been a comment by a non-author, we can remove the slow review label
  if (commentAuthor !== pullAuthor && commentAuthor !== BOT_NAME) {
    await removeSlowReviewLabel(payload);
  }
  if (processedCommand) {
    // If we've processed a command, don't worry about trying to change the attention set.
    // This is not a meaningful push or comment from the author.
    console.log("Processed command");
    return;
  }

  // If comment was from the author, we should shift attention back to the reviewers.
  console.log(
    "No command to be processed, checking if we should shift attention to reviewers"
  );
  if (pullAuthor === commentAuthor) {
    await setNextActionReviewers(payload, stateClient);
  } else {
    console.log(
      `Comment was from ${commentAuthor}, not author: ${pullAuthor}. No action to take.`
    );
  }
}

/*
 * On pr push or author comment, we should put the attention set back on the reviewers
 */
async function setNextActionReviewers(
  payload: any,
  stateClient: typeof PersistentState
) {
  const pullNumber = getPullNumberFromPayload(payload);
  if (!(await areReviewersAssigned(pullNumber, stateClient))) {
    console.log("No reviewers assigned, dont need to manipulate attention set");
    return;
  }
  const existingLabels = payload.issue?.labels || payload.pull_request?.labels;
  await nextActionReviewers(pullNumber, existingLabels);
  let prState = await stateClient.getPrState(pullNumber);
  prState.nextAction = REVIEWERS_ACTION;
  await stateClient.writePrState(pullNumber, prState);
}

async function processPrUpdate() {
  const reviewerConfig = new ReviewerConfig(PATH_TO_CONFIG_FILE);
  const context = github.context;
  console.log("Event context:");
  console.log(context);
  const payload = context.payload;

  if (!payload.issue?.pull_request && !payload.pull_request) {
    console.log("Issue, not pull request - returning");
    return;
  }
  const pullNumber = getPullNumberFromPayload(payload);

  const stateClient = new PersistentState();

  switch (github.context.eventName) {
    case "issue_comment":
      console.log("Processing comment event");
      if (payload.action !== "created") {
        console.log("Comment wasnt just created, skipping");
        return;
      }
      await processPrComment(payload, stateClient, reviewerConfig);
      break;
    case "pull_request_target":
      if (
        (await stateClient.getPrState(pullNumber)).stopReviewerNotifications
      ) {
        console.log("Notifications have been paused for this pull - skipping");
        return;
      }
      if (payload.action === "synchronize") {
        console.log("Processing synchronize action");
        await setNextActionReviewers(payload, stateClient);
      }
      // TODO(damccorm) - it would be good to eventually handle the following events here, even though they're not part of the normal workflow
      // review requested, assigned, label added, label removed
      break;
    default:
      console.log("Not a PR comment or push, doing nothing");
  }
}

processPrUpdate();

export {};
