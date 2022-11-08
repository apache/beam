const { ISSUES_MANAGER_TAG } = require("./constants.js");
const { getRepoWorkflows, getRunsForWorkflow } = require("./workflows.js");
const { getRepoIssues, closeIssue, createIssue} = require("./issues");


const checkConclusions = (conclusions) => {
  return ({ conclusion }) => {
    return conclusions.includes(conclusion);
  };
};

const filterRuns = (runs, conclusions) => {
  return runs.filter((run) => conclusions.includes(run.conclusion));
};

const splitWorkflows = async ({ github, context }, workflows) => {
  let lastKRuns = [];
  let unstable = [];
  let stable = [];
  let permared = [];

  //TODO: make it parallel
  for (const workflow of workflows) {
    const { workflow_runs } = await getRunsForWorkflow({ github, context }, workflow);
    let filteredRuns = filterRuns(workflow_runs, ["success", "failure", "timed_out"]);

    const output = filteredRuns.map(
      ({ id, name, conclusion, event, head_branch }) => `${id} | ${name} | ${conclusion} | ${event} |${head_branch}`
    );
    console.log("FILTERED WORKFLOW RUNS", output);

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

//TODO: Delete this function if issues can be obtained directly by a label
const filterIssuesByTag = (issues, tag) => {
  return issues.filter((issue) => issue.pull_request === undefined).filter((issue) => issue.title.startsWith(tag, 1));
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
  issues = filterIssuesByTag(issues, ISSUE_MANAGER_TAG); //Discards issues that are PRs and not created by the workflow issues manager

  const issuesByWorkflowId = getIssuesByWorkflowId(issues);

  for (const { workflow } of workflows) {
    let status = undefined;
    let issue_url = undefined;

    if (workflow.id in issuesByWorkflowId) {
      issue_url = issuesByWorkflowId[workflow.id].html_url;
      status = `EXISTENT -> ${issue_url}`;
    } else {
      issue_url = await createIssue({ github, context }, workflow);
      status = `CREATED -> ${issue_url}`;
    }

    results.push({ workflow, status });
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
    let status = undefined;
    let issue_url = undefined;

    if (workflow.id in issuesByWorkflowId) {
      let issue = issuesByWorkflowId[workflow.id];
      issue_url = await closeIssue({ github, context }, issue);
      status = `CLOSED ISSUE -> ${issue_url}`;
    } else {
      status = "NO ACTION";
    }

    results.push({ workflow, status });
  }

  return results;
};

module.exports = {
  getRepoWorkflows,
  splitWorkflows,
  createIssuesForWorkflows,
  closeIssuesForWorkflows,
};
