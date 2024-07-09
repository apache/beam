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

const github = require("./githubUtils");
const commentStrings = require("./commentStrings");
const { BOT_NAME } = require("./constants");
const { StateClient } = require("./persistentState");
const { ReviewerConfig } = require("./reviewerConfig");

// Reads the comment and processes the command if one is contained in it.
// Returns true if it runs a command, false otherwise.
export async function processCommand(
  payload: any,
  commentAuthor: string,
  commentText: string,
  stateClient: typeof StateClient,
  reviewerConfig: typeof ReviewerConfig
) {
  // Don't process any commands from our bot.
  if (commentAuthor === BOT_NAME) {
    return false;
  }
  console.log(commentAuthor);

  const pullNumber = payload.issue?.number || payload.pull_request?.number;
  commentText = commentText.toLowerCase();

  let prState = await stateClient.getPrState(pullNumber);
  if(prState.stopReviewerNotifications) {
    // Notifications stopped, only "allow assign set of reviewers"
    if (commentText.indexOf("assign set of reviewers") > -1) {
      await assignReviewerSet(payload, pullNumber, stateClient, reviewerConfig);
    } else {
      return false;
    }
  } else {
    if (commentText.indexOf("r: @") > -1) {
      await manuallyAssignedToReviewer(pullNumber, stateClient);
    } else if (commentText.indexOf("assign to next reviewer") > -1) {
      await assignToNextReviewer(
        payload,
        commentAuthor,
        pullNumber,
        stateClient,
        reviewerConfig
      );
    } else if (commentText.indexOf("stop reviewer notifications") > -1) {
      await stopReviewerNotifications(
        pullNumber,
        stateClient,
        "requested by reviewer"
      );
    } else if (commentText.indexOf("remind me after tests pass") > -1) {
      await remindAfterTestsPass(pullNumber, commentAuthor, stateClient);
    } else if (commentText.indexOf("waiting on author") > -1) {
      await waitOnAuthor(payload, pullNumber, stateClient);
    } else if (commentText.indexOf("assign set of reviewers") > -1) {
      await assignReviewerSet(payload, pullNumber, stateClient, reviewerConfig);
    } else {
      return false;
    }
  }
  return true;
}

async function assignToNextReviewer(
  payload: any,
  commentAuthor: string,
  pullNumber: number,
  stateClient: typeof StateClient,
  reviewerConfig: typeof ReviewerConfig
) {
  let prState = await stateClient.getPrState(pullNumber);
  let labelOfReviewer = prState.getLabelForReviewer(payload.sender.login);
  if (labelOfReviewer) {
    let reviewersState = await stateClient.getReviewersForLabelState(
      labelOfReviewer
    );
    const pullAuthor = github.getPullAuthorFromPayload(payload);
    let availableReviewers = reviewerConfig.getReviewersForLabel(
      labelOfReviewer,
      [commentAuthor, pullAuthor]
    );
    let chosenReviewer = reviewersState.assignNextReviewer(availableReviewers);
    prState.reviewersAssignedForLabels[labelOfReviewer] = chosenReviewer;

    // Comment assigning reviewer
    console.log(`Assigning ${chosenReviewer}`);
    await github.addPrComment(
      pullNumber,
      commentStrings.assignReviewer(prState.reviewersAssignedForLabels)
    );

    // Set next action to reviewer
    const existingLabels =
      payload.issue?.labels || payload.pull_request?.labels;
    await github.nextActionReviewers(pullNumber, existingLabels);
    prState.nextAction = "Reviewers";

    // Persist state
    await stateClient.writePrState(pullNumber, prState);
    await stateClient.writeReviewersForLabelState(
      labelOfReviewer,
      reviewersState
    );
  }
}

// If they've manually assigned a reviewer, just silence notifications and ignore this pr going forward.
// TODO(damccorm) - we could try to do something more intelligent here like figuring out which label that reviewer belongs to.
async function manuallyAssignedToReviewer(
  pullNumber: number,
  stateClient: typeof StateClient
) {
  await stopReviewerNotifications(
    pullNumber,
    stateClient,
    "review requested by someone other than the bot, ceding control"
  );
}

async function stopReviewerNotifications(
  pullNumber: number,
  stateClient: typeof StateClient,
  reason: string
) {
  let prState = await stateClient.getPrState(pullNumber);
  prState.stopReviewerNotifications = true;
  await stateClient.writePrState(pullNumber, prState);

  // Comment acknowledging command
  await github.addPrComment(
    pullNumber,
    commentStrings.stopNotifications(reason)
  );
}

async function remindAfterTestsPass(
  pullNumber: number,
  username: string,
  stateClient: typeof StateClient
) {
  let prState = await stateClient.getPrState(pullNumber);
  prState.remindAfterTestsPass.push(username);
  await stateClient.writePrState(pullNumber, prState);

  // Comment acknowledging command
  await github.addPrComment(
    pullNumber,
    commentStrings.remindReviewerAfterTestsPass(username)
  );
}

async function waitOnAuthor(
  payload: any,
  pullNumber: number,
  stateClient: typeof StateClient
) {
  const existingLabels = payload.issue?.labels || payload.pull_request?.labels;
  await github.nextActionAuthor(pullNumber, existingLabels);
  let prState = await stateClient.getPrState(pullNumber);
  prState.nextAction = "Author";
  await stateClient.writePrState(pullNumber, prState);
}

async function assignReviewerSet(
  payload: any,
  pullNumber: number,
  stateClient: typeof StateClient,
  reviewerConfig: typeof ReviewerConfig
) {
  let prState = await stateClient.getPrState(pullNumber);
  if(prState.stopReviewerNotifications) {
    // Restore notifications, and clear any existing reviewer set to
    // allow new reviewers to be assigned.
    prState.stopReviewerNotifications = false;
    prState.reviewersAssignedForLabels = {};
  }
  if (Object.values(prState.reviewersAssignedForLabels).length > 0) {
    await github.addPrComment(
      pullNumber,
      commentStrings.reviewersAlreadyAssigned(
        Object.values(prState.reviewersAssignedForLabels)
      )
    );
    return;
  }

  const existingLabels = payload.issue?.labels || payload.pull_request?.labels;
  const pullAuthor = github.getPullAuthorFromPayload(payload);
  const reviewersForLabels = reviewerConfig.getReviewersForLabels(
    existingLabels,
    [pullAuthor]
  );
  let reviewerStateToUpdate = {};
  var labels = Object.keys(reviewersForLabels);
  if (!labels || labels.length == 0) {
    await github.addPrComment(
      pullNumber,
      commentStrings.noLegalReviewers(existingLabels)
    );
    return;
  }
  for (let i = 0; i < labels.length; i++) {
    let label = labels[i];
    let availableReviewers = reviewersForLabels[label];
    let reviewersState = await stateClient.getReviewersForLabelState(label);
    let chosenReviewer = reviewersState.assignNextReviewer(availableReviewers);
    reviewerStateToUpdate[label] = reviewersState;
    prState.reviewersAssignedForLabels[label] = chosenReviewer;
  }
  console.log(`Assigning reviewers for pr ${pullNumber}`);
  await github.addPrComment(
    pullNumber,
    commentStrings.assignReviewer(prState.reviewersAssignedForLabels)
  );

  github.nextActionReviewers(pullNumber, existingLabels);
  prState.nextAction = "Reviewers";

  await stateClient.writePrState(pullNumber, prState);
  let labelsToUpdate = Object.keys(reviewerStateToUpdate);
  for (let i = 0; i < labelsToUpdate.length; i++) {
    let label = labelsToUpdate[i];
    await stateClient.writeReviewersForLabelState(
      label,
      reviewerStateToUpdate[label]
    );
  }
}
