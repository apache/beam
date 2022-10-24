const {DEFAULT_K, MIN_RUNS, ISSUE_MANAGER_TAG} = require('./constants.js');

const getRepoWorkflows = async ({ github, context }) => {
  const { data: { workflows } } = await github.rest.actions.listRepoWorkflows({
    owner: 'apache',
    repo: 'beam',
    per_page: 100
  });

  return workflows;
};

const getRunsForWorkflow = async ({github, context}, workflow) => {
  const k = MIN_RUNS[workflow.name] || MIN_RUNS[workflow.id] || DEFAULT_K;

  // K should be less than 100 or the request should be paginated
  if(k > 100) {
    throw new Error("K should be less than 100");
  }

  const { data: { workflow_runs } } = await github.rest.actions.listWorkflowRuns({
    owner: 'apache',
    repo: context.repo.repo,
    workflow_id: workflow.id,
    status: 'completed',
    branch: 'master',
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

const getRepoIssues = async ({github, context}) => {
  let issues = [];

  //TODO: Clean or make it different
  //TODO: Use a label or an id to get more specific issues
  for await (const response of github.paginate.iterator(github.rest.issues.listForRepo, {
    owner: "apache",
    repo: "beam",
    per_page: 100,
  })) {
    issues = issues.concat(response.data);
  }

  return issues;
};

const checkConclusions = (conclusions) => {
  return ({ conclusion }) => {
    // console.log('CONCLUSION', `${conclusion} -> ${conclusions} = ${conclusions.includes(conclusion)}`);
    return conclusions.includes(conclusion);
  };
};

const filterRuns = (runs, conclusions) => {
  return runs.filter(run => conclusions.includes(run.conclusion));
}

const splitWorkflows = async ({ github, context }, workflows) => {
  let lastKRuns = [];
  let unstable = [];
  let stable = [];
  let permared = [];

  //TODO: make it parallel
  for (const workflow of workflows) {
    const { workflow_runs } = await getRunsForWorkflow({github, context}, workflow);

    let filteredRuns = filterRuns(workflow_runs, ['success', 'failure', 'timed_out']);
    const output = filteredRuns.map(({id, name, conclusion, event, head_branch}) => `${id} | ${name} | ${conclusion} | ${event} |${head_branch}`);
    console.log("FILTERED WORKFLOW RUNS", output);

    lastKRuns.push({
      workflow,
      filteredRuns
    });
  }

  const isSuccessful = checkConclusions(['success']);
  const isFailure = checkConclusions(['failure', 'timed_out']);

  //TODO: Make it in a reduce
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
  return issues
    .filter((issue) => issue.pull_request === undefined)
    .filter((issue) => issue.title.startsWith(tag, 1));
};

const getIssuesByWorkflowId = (issues) => {
  return issues.reduce((prev, curr) => {
    const workflowId = getWorkflowIdFromIssueTitle(curr.title)
    return {
      ...prev,
      [workflowId]: curr,
    };
  }, {});
};

const createIssuesForWorkflows = async ({ github, context }, workflows) => {
  let issues = [];
  let results = [];

  issues = await getRepoIssues({github, context});
  issues = filterIssuesByTag(issues, ISSUE_MANAGER_TAG);  //Discards issues that are PRs and not created by the workflow issues manager

  const issuesByWorkflowId = getIssuesByWorkflowId(issues);

  for(const { workflow } of workflows) {
    let status = "";
    let issue_url = undefined;

    if (workflow.id in issuesByWorkflowId) {
      // issue_url = issuesByWorkflowId[workflow.id].html_url;
      status = `EXISTENT -> ${issue_url}`;
    }
    else {
      // issue_url = await createIssue({github, context}, workflow);
      status = `CREATED -> ${issue_url}`;
    }

    results.push({ workflow, status});
  }

  return results;
};

const closeIssuesForWorkflows = async ({ github, context }, workflows) => {
  let issues = [];
  let results = [];

  issues = await getRepoIssues({github, context});
  issues = filterIssuesByTag(issues, ISSUE_MANAGER_TAG);  //Discards issues that are PRs and not created by the workflow issues manager

  const issuesByWorkflowId = getIssuesByWorkflowId(issues);

  for(const { workflow } of workflows) {
    let status = "";
    let issue_url = undefined;

    if (workflow.id in issuesByWorkflowId) {
      // let issue = issuesByWorkflowId[workflow.id];
      // issue_url = await closeIssue({github, context}, issue);
      status = `CLOSED ISSUE -> ${issue_url}`;
    }
    else {
      status = `NO ACTION`;
    }

    results.push({ workflow, status});
  }

  return results;
};

const createIssue = async ({github, context}, workflow) => {
  //Issue needs to have the following format so it can be found by the bot
  const fileName = workflow.path.split('/').pop();
  const issueTitle = `[${ISSUE_MANAGER_TAG}][${workflow.id}]: ${workflow.name}`;
  const issueBody = `
        THIS ISSUE WAS CREATED AUTOMATICALLY AS PART OF A NEW BOT
        PLEASE IGNORE IT

        Workflow URL: ${context.repo}/actions/workflows/${fileName}
        HTML URL: ${workflow.html_url}
  `;

  // TODO: add labels from default labels
  const issue = await github.rest.issues.create({
    owner: github.repo.owner,
    repo: github.repo.repo,
    title: issueTitle,
    body: issueBody
  });

  return issue.html_url;
};

const closeIssue = async ({github, context}, issue) => {
  let issueResult = await github.rest.issues.update({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: issue.number,
    state: 'closed'
  });

  return issueResult.html_url;
};

module.exports = {
  getRepoWorkflows,
  splitWorkflows,
  createIssuesForWorkflows,
  closeIssuesForWorkflows
};
