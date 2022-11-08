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
    owner: "apache",
    repo: context.repo.repo,
    workflow_id: workflow.id,
    status: "completed",
    branch: "master",
    // event: 'schedule', Filter by schedule and push
    per_page: k,
    page: 1,
  });

  // const output = workflow_runs.map(({id, name, conclusion, event, head_branch}) => `${id} ${name} ${conclusion} ${event} ${head_branch}`);
  // console.log("WORKFLOW RUNS", output);

  return {
    workflow,
    workflow_runs,
  };
};

module.exports = { getRepoWorkflows, getRunsForWorkflow };
