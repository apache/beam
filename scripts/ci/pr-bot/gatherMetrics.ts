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

const fs = require("fs");
const github = require("./shared/githubUtils");
const {
  REPO_OWNER,
  REPO,
  PATH_TO_METRICS_CSV,
  BOT_NAME,
} = require("./shared/constants");

interface PrStats {
  firstTimeContribution: boolean;
  timeToFirstReview: number;
  timeFromCreationToCompletion: number;
  timeFromReviewersMentionedToCompletion: { [key: string]: number };
  mergedDate: Date;
}

interface AggregatedMetrics {
  prsCompleted: number;
  prsCompletedByNewContributors: number;
  averageTimeToFirstReview: number;
  averageTimeToNewContributorFirstReview: number;
  averageTimeCreationToCompletion: number;
  numUsersPerformingReviews: number;
  numCommittersPerformingReviews: number;
  numNonCommittersPerformingReviews: number;
  giniIndexCommittersPerformingReviews: number;
  averageTimeFromCommitterAssignmentToPrMerge: number;
}

async function getCompletedPullsFromLastYear(): Promise<any[]> {
  const cutoffDate = new Date();
  cutoffDate.setFullYear(new Date().getFullYear() - 1);
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
  console.log("Got all PRs, moving to the processing stage");
  return pulls;
}

// Rather than check the whole repo history (expensive),
// we'll just look at the last year's worth of contributions to check if they're a first time contributor.
// If they contributed > 1 year ago, their experience is probably similar to a first time contributor anyways.
function checkIfFirstTimeContributor(
  pull: any,
  pullsFromLastYear: any[]
): boolean {
  return !pullsFromLastYear.some(
    (pullFromLastYear) =>
      pullFromLastYear.created_at < pull.created_at &&
      pullFromLastYear.user.login === pull.user.login
  );
}

// Get time between pr creation and the first comment, approval, or merge that isn't done by:
// (a) the author
// (b) automated tooling
function getTimeToFirstReview(
  pull: any,
  comments: any[],
  reviews: any[],
  creationDate: Date,
  mergedDate: Date
): number {
  let timeToFirstReview = mergedDate.getTime() - creationDate.getTime();

  const firstReviewed = reviews.find(
    (review) => review.user.login != pull.user.login
  );
  if (firstReviewed) {
    const firstReviewDate = new Date(firstReviewed.submitted_at);
    timeToFirstReview = Math.min(
      timeToFirstReview,
      firstReviewDate.getTime() - creationDate.getTime()
    );
  }
  for (const comment of comments) {
    if (
      comment.user.login != pull.user.login &&
      comment.user.login != BOT_NAME
    ) {
      let commentTime = new Date(comment.created_at);
      timeToFirstReview = Math.min(
        timeToFirstReview,
        commentTime.getTime() - creationDate.getTime()
      );
    }
  }
  return timeToFirstReview;
}

// Takes a R: @reviewer comment and extracts all reviewers tagged
// Returns an empty list if no reviewer can be extracted
function extractReviewersTaggedFromCommentBody(body: string): string[] {
  if (!body) {
    return [];
  }
  body = body.toLowerCase();
  if (body.indexOf("r: @") < 0) {
    return [];
  }
  let usernames: string[] = [];
  const reviewerStrings = body.split(" @");
  // Start at index 1 since we don't care about anything before the first @
  for (let i = 1; i < reviewerStrings.length; i++) {
    const curBlock = reviewerStrings[i];
    let usernameIndex = 0;
    let curUsername = "";
    while (
      usernameIndex < curBlock.length &&
      curBlock[usernameIndex].match(/^[0-9a-z]+$/)
    ) {
      curUsername += curBlock[usernameIndex];
      usernameIndex += 1;
    }
    // Filter out username from PR template
    if (curUsername && curUsername != "username") {
      usernames.push(curUsername);
    }
  }

  return usernames;
}

// Returns a dictionary mapping reviewers to the amount of time from their first comment to pr completion.
function getTimeFromReviewerMentionedToCompletion(
  pull: any,
  comments: any[],
  reviewComments: any[],
  mergedDate: Date
): { [key: string]: number } {
  comments = comments.concat(reviewComments);
  comments.push(pull);
  let timeToCompletionPerReviewer = {};
  for (const comment of comments) {
    const reviewersTagged = extractReviewersTaggedFromCommentBody(comment.body);
    const commentCreationDate = new Date(comment.created_at);
    const timeToCompletion =
      mergedDate.getTime() - commentCreationDate.getTime();
    for (const reviewer of reviewersTagged) {
      if (reviewer in timeToCompletionPerReviewer) {
        timeToCompletionPerReviewer[reviewer] = Math.max(
          timeToCompletion,
          timeToCompletionPerReviewer[reviewer]
        );
      } else {
        timeToCompletionPerReviewer[reviewer] = timeToCompletion;
      }
    }
  }

  return timeToCompletionPerReviewer;
}

async function extractPrStats(
  pull: any,
  pullsFromLastYear: any[]
): Promise<PrStats> {
  const githubClient = github.getGitHubClient();
  const creationDate = new Date(pull.created_at);
  const mergedDate = new Date(pull.merged_at);
  const reviews = (
    await githubClient.rest.pulls.listReviews({
      owner: REPO_OWNER,
      repo: REPO,
      pull_number: pull.number,
    })
  ).data;
  // GitHub has a concept of review comments (must be part of a review) and issue comments on a repo, so we need to look at both
  const comments = (
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
  const prStats: PrStats = {
    firstTimeContribution: checkIfFirstTimeContributor(pull, pullsFromLastYear),
    timeToFirstReview: getTimeToFirstReview(
      pull,
      comments,
      reviews,
      creationDate,
      mergedDate
    ),
    timeFromCreationToCompletion: mergedDate.getTime() - creationDate.getTime(),
    timeFromReviewersMentionedToCompletion:
      getTimeFromReviewerMentionedToCompletion(
        pull,
        comments,
        reviewComments,
        mergedDate
      ),
    mergedDate: mergedDate,
  };

  return prStats;
}

function getMetricBucketStartDate(pullStat: PrStats, bucketEnd: Date): number {
  const bucketStart = bucketEnd;
  while (bucketStart.getTime() > pullStat.mergedDate.getTime()) {
    bucketStart.setDate(bucketStart.getDate() - 7);
  }

  return bucketStart.getTime();
}

function distinctReviewers(pullStats: PrStats[]): string[] {
  let users: Set<string> = new Set();
  for (const pullStat of pullStats) {
    for (const user of Object.keys(
      pullStat.timeFromReviewersMentionedToCompletion
    )) {
      users.add(user);
    }
  }
  return Array.from(users);
}

async function committersFromReviewers(users: string[]): Promise<string[]> {
  let committers: string[] = [];
  for (const user of users) {
    if (await github.checkIfCommitter(user)) {
      committers.push(user);
    }
  }
  return committers;
}

function averageTimeFromCommitterAssignmentToPrMerge(
  pullStats: PrStats[],
  committers: string[]
): number {
  if (committers.length === 0) {
    return 0;
  }
  let numCommitterReviews = 0;
  let totalTimeFromAssignToMerge = 0;
  for (const pullStat of pullStats) {
    for (const reviewer of Object.keys(
      pullStat.timeFromReviewersMentionedToCompletion
    )) {
      if (committers.indexOf(reviewer) > -1) {
        numCommitterReviews++;
        totalTimeFromAssignToMerge +=
          pullStat.timeFromReviewersMentionedToCompletion[reviewer];
      }
    }
  }
  if (numCommitterReviews === 0) {
    return 0;
  }

  return totalTimeFromAssignToMerge / numCommitterReviews;
}

// Calculates a gini index of inequality for reviews.
// 0 is perfectly equally distributed, 1 is inequally distributed (with 1 person having all reviews)
function getGiniIndexForCommitterReviews(
  pullStats: PrStats[],
  committers: string[]
) {
  let reviewsPerCommitter: { [key: string]: number } = {};
  for (const pullStat of pullStats) {
    for (const reviewer of Object.keys(
      pullStat.timeFromReviewersMentionedToCompletion
    )) {
      if (committers.indexOf(reviewer) > -1) {
        if (reviewer in reviewsPerCommitter) {
          reviewsPerCommitter[reviewer]++;
        } else {
          reviewsPerCommitter[reviewer] = 1;
        }
      }
    }
  }
  let reviewCounts = Object.values(reviewsPerCommitter);
  reviewCounts.sort();
  let giniNumerator = 0;
  let giniDenominator = 0;
  const n = reviewCounts.length;
  for (let i = 1; i <= reviewCounts.length; i++) {
    let yi = reviewCounts[i - 1];
    giniNumerator += (n + 1 - i) * yi;
    giniDenominator += yi;
  }
  return (1 / n) * (n + 1 - (2 * giniNumerator) / giniDenominator);
}

async function aggregateStatsForBucket(
  pullStats: PrStats[]
): Promise<AggregatedMetrics> {
  const reviewers = distinctReviewers(pullStats);
  const committers = await committersFromReviewers(reviewers);
  const firstTimePrs = pullStats.filter(
    (pullStat) => pullStat.firstTimeContribution
  );
  let averageTimeToNewContributorFirstReview = 0;
  if (firstTimePrs.length > 0) {
    averageTimeToNewContributorFirstReview =
      firstTimePrs.reduce(
        (sumTime, prStat) => sumTime + prStat.timeToFirstReview,
        0
      ) / firstTimePrs.length;
  }
  return {
    prsCompleted: pullStats.length,
    prsCompletedByNewContributors: firstTimePrs.length,
    averageTimeToFirstReview:
      pullStats.reduce(
        (sumTime, prStat) => sumTime + prStat.timeToFirstReview,
        0
      ) / pullStats.length,
    averageTimeToNewContributorFirstReview:
      averageTimeToNewContributorFirstReview,
    averageTimeCreationToCompletion:
      pullStats.reduce(
        (sumTime, prStat) => sumTime + prStat.timeFromCreationToCompletion,
        0
      ) / pullStats.length,
    numUsersPerformingReviews: reviewers.length,
    numCommittersPerformingReviews: committers.length,
    numNonCommittersPerformingReviews: reviewers.length - committers.length,
    giniIndexCommittersPerformingReviews: getGiniIndexForCommitterReviews(
      pullStats,
      committers
    ),
    averageTimeFromCommitterAssignmentToPrMerge:
      averageTimeFromCommitterAssignmentToPrMerge(pullStats, committers),
  };
}

function convertMsToRoundedMinutes(milliseconds: number): number {
  return Math.floor(milliseconds / 60_000);
}

async function reportMetrics(statBuckets: { [key: number]: PrStats[] }) {
  console.log("---------------------------------");
  console.log("PR Stats");
  console.log("---------------------------------");
  let csvOutput = "";
  csvOutput +=
    "Bucket start (bucketed by merge time),PRs Completed,PRs completed by first time contributors,Average time in minutes to first review,Average time in minutes to first review for new contributors,Average time in minutes from PR creation to completion,Total number of reviewers,Total number of committers performing reviews,Total number of non-committers performing reviews,Gini index (fairness) of committers performing reviews,Average time in minutes from committer assignment to PR merge";
  const startDates = Object.keys(statBuckets);
  for (let i = 0; i < startDates.length; i++) {
    let startDate = startDates[i];
    let aggregatedStats = await aggregateStatsForBucket(statBuckets[startDate]);
    console.log();

    const bucketStart = new Date(parseInt(startDate));
    console.log("Bucket start:", bucketStart);
    csvOutput += `\n${bucketStart.toDateString()}`;

    console.log("PRs completed:", aggregatedStats.prsCompleted);
    csvOutput += `,${aggregatedStats.prsCompleted}`;

    console.log(
      "PRs completed by first time contributors:",
      aggregatedStats.prsCompletedByNewContributors
    );
    csvOutput += `,${aggregatedStats.prsCompletedByNewContributors}`;

    console.log(
      "Average time in minutes to first review:",
      convertMsToRoundedMinutes(aggregatedStats.averageTimeToFirstReview)
    );
    csvOutput += `,${convertMsToRoundedMinutes(
      aggregatedStats.averageTimeToFirstReview
    )}`;

    console.log(
      "Average time in minutes to first review for new contributors:",
      convertMsToRoundedMinutes(
        aggregatedStats.averageTimeToNewContributorFirstReview
      )
    );
    csvOutput += `,${convertMsToRoundedMinutes(
      aggregatedStats.averageTimeToNewContributorFirstReview
    )}`;

    console.log(
      "Average time in minutes from PR creation to completion:",
      convertMsToRoundedMinutes(aggregatedStats.averageTimeCreationToCompletion)
    );
    csvOutput += `,${convertMsToRoundedMinutes(
      aggregatedStats.averageTimeCreationToCompletion
    )}`;

    console.log(
      "Total number of reviewers:",
      aggregatedStats.numUsersPerformingReviews
    );
    csvOutput += `,${aggregatedStats.numUsersPerformingReviews}`;

    console.log(
      "Total number of committers performing reviews:",
      aggregatedStats.numCommittersPerformingReviews
    );
    csvOutput += `,${aggregatedStats.numCommittersPerformingReviews}`;

    console.log(
      "Total number of non-committers performing reviews:",
      aggregatedStats.numNonCommittersPerformingReviews
    );
    csvOutput += `,${aggregatedStats.numNonCommittersPerformingReviews}`;

    console.log(
      "Gini index (fairness) of committers performing reviews:",
      aggregatedStats.giniIndexCommittersPerformingReviews
    );
    csvOutput += `,${aggregatedStats.giniIndexCommittersPerformingReviews}`;

    console.log(
      "Average time in minutes from committer assignment to PR merge:",
      convertMsToRoundedMinutes(
        aggregatedStats.averageTimeFromCommitterAssignmentToPrMerge
      )
    );
    csvOutput += `,${convertMsToRoundedMinutes(
      aggregatedStats.averageTimeFromCommitterAssignmentToPrMerge
    )}`;
  }
  fs.writeFileSync(PATH_TO_METRICS_CSV, csvOutput);
  console.log(`Output written to ${PATH_TO_METRICS_CSV}`);
}

async function gatherMetrics() {
  // We will only aggregate metrics from the last 90 days,
  // but will look further back to determine if this is a user's first contribution
  const pullsFromLastYear = await getCompletedPullsFromLastYear();
  let cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - 90);

  let pullStats: PrStats[] = [];
  console.log("Extracting stats from pulls - this may take a while");
  for (let i = 0; i < pullsFromLastYear.length; i++) {
    let pull = pullsFromLastYear[i];
    if (new Date(pull.created_at) > cutoffDate && pull.merged_at) {
      pullStats.push(await extractPrStats(pull, pullsFromLastYear));
    }
    if (i % 10 === 0) {
      process.stdout.write(".");
    }
  }

  console.log("\nDone extracting stats, formatting results");

  let statBuckets: { [key: number]: PrStats[] } = {};
  let bucketEnd = new Date();
  bucketEnd.setUTCHours(23, 59, 59, 999);
  pullStats.forEach((pullStat) => {
    let bucketStart = getMetricBucketStartDate(pullStat, bucketEnd);
    if (bucketStart in statBuckets) {
      statBuckets[bucketStart].push(pullStat);
    } else {
      statBuckets[bucketStart] = [pullStat];
    }
  });
  await reportMetrics(statBuckets);
}

gatherMetrics();

export {};
