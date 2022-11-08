const { ISSUE_MANAGER_TAG } = require("./constants.js");

const getRepoIssues = async ({ github, context }) => {
  let issues = [];

  //TODO: Use a label or an id to get more specific issues
  for await (const response of github.paginate.iterator(github.rest.issues.listForRepo, {
    owner: context.repo.owner,
    repo: context.repo.repo,
    per_page: 100,
  })) {
    issues = issues.concat(response.data);
  }

  return issues;
};

const createIssue = async ({ github, context }, workflow) => {
  //Issue needs to have the following format so it can be found by the bot
  const fileName = workflow.path.split("/").pop();
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
    body: issueBody,
  });

  return issue.html_url;
};

const closeIssue = async ({ github, context }, issue) => {
  let issueResult = await github.rest.issues.update({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: issue.number,
    state: "closed",
  });

  return issueResult.html_url;
};

module.exports = { getRepoIssues, createIssue, closeIssue };
