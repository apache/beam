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

const { ISSUES_MANAGER_TAG, ISSUE_STATUS } = require("./constants.js");
const { getRepoWorkflows, getRunsForWorkflow } = require("./workflows.js");
const { getRepoIssues, closeIssue, createIssue } = require("./issues");

const checkConclusions = (conclusions) => {
  return ({ conclusion }) => {
    return conclusions.includes(conclusion);
  };
};

const filterRuns = (runs, conclusions, events) => {
  return runs.filter(({conclusion, event}) => conclusions.includes(conclusion) && events.includes(event));
};

const splitWorkflows = async ({ github, context }, workflows) => {
  let lastKRuns = [];
  let unstable = [];
  let stable = [];
  let permared = [];

  for (const workflow of workflows) {
    //Gets runs for an specific workflow on master, triggered by push and schedule events, with the
    //given conclusions (skipped, canceled, etc., are omitted)
    const { workflow_runs } = await getRunsForWorkflow({ github, context }, workflow);
    let filteredRuns = filterRuns(workflow_runs, ["success", "failure", "timed_out"], ["push", 'schedule']);

    console.log("\n", workflow.name);
    console.table(filteredRuns, ["id", "conclusion", "event", "head_branch"]);

    lastKRuns.push({
      workflow,
      filteredRuns,
    });
  }

  const isSuccessful = checkConclusions(["success"]);
  const isFailure = checkConclusions(["failure", "timed_out"]);

  //TODO: Handle case when filteredRuns is empty

  lastKRuns = lastKRuns.filter(({ filteredRuns }) => filteredRuns.length > 0);

  unstable = lastKRuns.filter(({ filteredRuns }) => filteredRuns.some(isFailure) && !filteredRuns.every(isFailure));
  stable = lastKRuns.filter(({ filteredRuns }) => filteredRuns.every(isSuccessful));
  permared = lastKRuns.filter(({ filteredRuns }) => filteredRuns.every(isFailure));

  return { stable, unstable, permared };
};

const getWorkflowIdFromIssueTitle = (title) => {
  return title.match(/\[(\d*)\]/).pop();
};

const getIssuesByWorkflowId = (issues) => {
  return issues.reduce((prev, curr) => {
    const workflowId = getWorkflowIdFromIssueTitle(curr.title);
    return {
      ...prev,
      [workflowId]: curr,
    };
  }, {});
};

const createIssuesForWorkflows = async ({ github, context }, workflows) => {
  let issues = [];
  let results = [];

  issues = await getRepoIssues({ github, context });

  const issuesByWorkflowId = getIssuesByWorkflowId(issues);

  for (const { workflow } of workflows) {
    let issue_status, issue_url;

    if (workflow.id in issuesByWorkflowId) {
      issue_url = issuesByWorkflowId[workflow.id].html_url;
      issue_status = ISSUE_STATUS.EXISTENT;
    } else {
      issue_url = await createIssue({ github, context }, workflow);
      issue_status = ISSUE_STATUS.CREATED;
    }

    results.push({ ...workflow, issue_status, issue_url });
  }

  return results;
};

const closeIssuesForWorkflows = async ({ github, context }, workflows) => {
  let issues = [];
  let results = [];

  issues = await getRepoIssues({ github, context });
  issues = filterIssuesByTag(issues, ISSUES_MANAGER_TAG); //Discards issues that are PRs and not created by the workflow issues manager

  const issuesByWorkflowId = getIssuesByWorkflowId(issues);

  for (const { workflow } of workflows) {
    let issue_status, issue_url;

    if (workflow.id in issuesByWorkflowId) {
      let issue = issuesByWorkflowId[workflow.id];
      issue_url = await closeIssue({ github, context }, issue);
      issue_status = ISSUE_STATUS.CLOSED;
    } else {
      issue_status = ISSUE_STATUS.SKIPPED;
    }

    results.push({ ...workflow, issue_status, issue_url });
  }

  return results;
};

module.exports = {
  getRepoWorkflows,
  splitWorkflows,
  createIssuesForWorkflows,
  closeIssuesForWorkflows,
};
