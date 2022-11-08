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

const { DEFAULT_K, MIN_RUNS } = require("./constants.js");

const getRepoWorkflows = async ({ github, context }) => {
  const {
    data: { workflows },
  } = await github.rest.actions.listRepoWorkflows({
    owner: context.repo.owner,
    repo: context.repo.repo,
    per_page: 100,
  });

  return workflows;
};

const getRunsForWorkflow = async ({ github, context }, workflow) => {
  const k = MIN_RUNS[workflow.name] || MIN_RUNS[workflow.id] || DEFAULT_K;

  // K should be less than 100 or the request should be paginated
  if (k > 100) {
    throw new Error("K should be less than 100");
  }

  const {
    data: { workflow_runs },
  } = await github.rest.actions.listWorkflowRuns({
    owner: context.repo.owner,
    repo: context.repo.repo,
    workflow_id: workflow.id,
    status: "completed",
    branch: "master",
    per_page: k,
    page: 1,
  });

  return {
    workflow,
    workflow_runs,
  };
};

module.exports = { getRepoWorkflows, getRunsForWorkflow };
