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

const { NO_MATCHING_LABEL } = require("./constants");

export function allChecksPassed(reviewersToNotify: string[]): string {
  return `All checks have passed: @${reviewersToNotify.join(" ")}`;
}

export function assignCommitter(committer: string): string {
  return `R: @${committer} for final approval`;
}

export function assignReviewer(labelToReviewerMapping: any): string {
  let commentString =
    "Assigning reviewers. If you would like to opt out of this review, comment `assign to next reviewer`:\n\n";

  for (let label in labelToReviewerMapping) {
    let reviewer = labelToReviewerMapping[label];
    if (label === NO_MATCHING_LABEL) {
      commentString += `R: @${reviewer} added as fallback since no labels match configuration\n`;
    } else {
      commentString += `R: @${reviewer} for label ${label}.\n`;
    }
  }

  commentString += `
Available commands:
- \`stop reviewer notifications\` - opt out of the automated review tooling
- \`remind me after tests pass\` - tag the comment author after tests pass
- \`waiting on author\` - shift the attention set back to the author (any comment or push by the author will return the attention set to the reviewers)

The PR bot will only process comments in the main thread (not review comments).`;
  return commentString;
}

export function failingChecksCantAssign(): string {
  return "Checks are failing. Will not request review until checks are succeeding. If you'd like to override that behavior, comment `assign set of reviewers`";
}

export function someChecksFailing(reviewersToNotify: string[]): string {
  return `Some checks have failed: @${reviewersToNotify.join(" ")}`;
}

export function stopNotifications(reason: string): string {
  return `Stopping reviewer notifications for this pull request: ${reason}. If you'd like to restart, comment \`assign set of reviewers\``;
}

export function remindReviewerAfterTestsPass(requester: string): string {
  return `Ok - I'll remind @${requester} after tests pass`;
}

export function reviewersAlreadyAssigned(reviewers: string[]): string {
  return `Reviewers are already assigned to this PR: ${reviewers
    .map((reviewer) => "@" + reviewer)
    .join(" ")}`;
}

export function noLegalReviewers(): string {
  return "No reviewers could be found from any of the labels on the PR or in the fallback reviewers list. Check the config file to make sure reviewers are configured";
}

export function updateReviewerConfig(
  reviewersAddedForLabels: { [reviewer: string]: string[] },
  reviewersRemovedForLabels: { [reviewer: string]: string[] }
) {
  let commentString = `Adds and/or removes reviewers based on activity in the repo.
If you have been added and would prefer not to be, you can avoid getting repeatedly suggested by adding yourself to that label's exclusionList.
`;

  if (Object.keys(reviewersAddedForLabels).length > 0) {
    commentString += `
The following users have been added as reviewers to the configuration.
If you choose to accept being added, you will be added to the rotation of users who are automatically added to pull requests for an initial review.
A committer will still have to approve after your review (if you are not a committer), but you will be the initial touchpoint for PRs to which you are assigned.
`;
    for (const reviewer of Object.keys(reviewersAddedForLabels)) {
      commentString += `${reviewer} added for label(s): ${reviewersAddedForLabels[
        reviewer
      ].join(",")}\n`;
    }
  }

  if (Object.keys(reviewersRemovedForLabels).length > 0) {
    commentString += `
The following users have been removed as reviewers from the configuration.
Users are removed if they haven't reviewed or completed a PR in the last 3 months.
`;
    for (const reviewer of Object.keys(reviewersRemovedForLabels)) {
      commentString += `@${reviewer} removed for label(s): ${reviewersRemovedForLabels[
        reviewer
      ].join(",")}\n`;
    }
  }

  return commentString;
}

export function assignNewReviewer(labelToReviewerMapping: {
  [label: string]: string;
}): string {
  let commentString =
    "Assigning new set of reviewers because Pr has gone too long without review. If you would like to opt out of this review, comment `assign to next reviewer`:\n\n";

  for (const label in labelToReviewerMapping) {
    const reviewer = labelToReviewerMapping[label];
    if (label === NO_MATCHING_LABEL) {
      commentString += `R: @${reviewer} added as fallback since no labels match configuration\n`;
    } else {
      commentString += `R: @${reviewer} for label ${label}.\n`;
    }
  }

  commentString += `
Available commands:
- \`stop reviewer notifications\` - opt out of the automated review tooling
- \`remind me after tests pass\` - tag the comment author after tests pass
- \`waiting on author\` - shift the attention set back to the author (any comment or push by the author will return the attention set to the reviewers)`;
  return commentString;
}

export function slowReview(reviewers: string[]): string {
  let commentString = `Reminder, please take a look at this pr: `;
  for (const reviewer of reviewers) {
    commentString += `@${reviewer} `;
  }

  return commentString;
}
