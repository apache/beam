<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

> **PLEASE update this file if you add new GitHub Action or change name/trigger phrase of a GitHub Action.**
## About GitHub Actions Runners and Self-hosted Runners
According to GitHub Docs, we can define a GitHub-hosted runner and a self-hosted runner as the following:
* A [GitHub-hosted runner](https://docs.github.com/en/actions/using-github-hosted-runners/about-github-hosted-runners) is a new virtual machine (VM) hosted by GitHub with the runner application and other tools preinstalled, and is available with Ubuntu Linux, Windows, or macOS operating systems.
* A [self-hosted runner](https://docs.github.com/en/actions/hosting-your-own-runners/about-self-hosted-runners) is a system that you deploy and manage to execute jobs from GitHub Actions on GitHub.com.

## Apache Beam GitHub Actions

Currently, we have both GitHub-hosted and self-hosted runners for running the GitHub Actions workflows, hosted on Google Cloud Platform(GCP) Virtual Machines and Google Kubernetes Engine(GKE). The majority of our workflows that run in Ubuntu and Windows run in self-hosted runners, except for those that runs on MacOS and the `Monitor Self-Hosted Runners Status` workflow that monitors our GCP self-hosted runners.

### Getting Started with self-hosted runners
* Refer to [this README](./gh-actions-self-hosted-runners/README.md) for the steps for creating your own self-hosted runners for testing your workflows.
* Depending on your workflow's needs, it must specify the following `runs-on` tags to run in the specified operating system:
  * Ubuntu 20.04 self-hosted runner: `[self-hosted, ubuntu-20.04]`
  * Windows Server 2019 self-hosted runner: `[self-hosted, windows-server-2019]`
  * MacOS GitHub-hosted runner: `macos-latest`
* Every workflow that tests the source code, needs to have the workflow trigger `pull_request_target` instead of `pull_request`.
* The workflow must have set read permissions for all the available scopes and jobs: `permissions: read-all`. It must be set at the top of the `jobs` directive.
* For those workflows that have the `pull_request_target` trigger, in the checkout step must be added a ref to `${{ github.event.pull_request.head.sha }}`
``` yaml
    - name: Checkout code
      uses: actions/checkout@v#
      with:
        ref: ${{ github.event.pull_request.head.sha }}
```
* If your workflow runs successfully in a GitHub-hosted runner but not in the self-hosted runner, it might need a new installation step.
```yaml
  - name: Setup Node
    uses: actions/setup-node@v3
    with:
      node-version: 16
```
* You can find the GitHub-hosted runner installations in the following links:
  * [Ubuntu-20.04](https://github.com/actions/runner-images/blob/main/images/linux/Ubuntu2004-Readme.md#installed-apt-packages)
  * [Windows-2019](https://github.com/actions/runner-images/blob/main/images/win/Windows2019-Readme.md)

#### GitHub Actions Example
```yaml
name: GitHub Actions Example
on:
  pull_request_target:
    branches: ['master']
permissions: read-all
jobs:
  github-actions-example:
    runs-on: [self-hosted, ubuntu-20.04]
    steps:
      - name: Check out repository code
        uses: actions/checkout@v2
        with:
          ref: ${{ github.event.pull_request.head.sha }}
      - run: echo "This job is now running on a ubuntu server hosted by Apache Beam!"
      - name: Setup Node
          uses: actions/setup-node@v3
          with:
            node-version: 16
          - name: Install npm dependencies
            run: npm ci
            working-directory: 'scripts/ci/your-path'
          - name: Run Node.js code
            run: npm run functionName
            env:
              VAR_1: my-var
            working-directory: 'scripts/ci/your-path'
```

#### IMPORTANT for Committers
* A **detailed review** for changes in the workflows is needed due to important **security concerns**.
* **DO NOT** Approve and Run changes in the workflows in the PR Conversation tab, under "Workflow(s) awaiting approval".
* For approving the updates in the workflows, you should go to the Repository Actions and filter All Workflows by `action_required`. The search will display the workflows that need to be reviewed before running. **Please make sure reviewing the file that is referenced by the workflow.**
* Seed job will be emulated using the `Approve and Run` built-in feature of GitHub Actions, since the workflows will use the `pull_request_target` directive; no modifications would be allowed either for new or existent jobs unless a committer explicitly approves the job from GitHub Actions UI.
### Issue Management

Phrases self-assign, close, or manage labels on an issue:
| Phrase | Effect |
|--------|--------|
| `.take-issue` | Self-assign the issue |
| `.close-issue` | Close the issue as completed |
| `.close-issue not_planned` | Close the issue as not-planned |
| `.reopen-issue` | Reopen a closed issue |
| `.add-labels` | Add comma separated labels to the issue (e.g. `add-labels l1, 'l2 with spaces'`) |
| `.remove-labels` | Remove comma separated labels to the issue (e.g. `remove-labels l1, 'l2 with spaces'`) |
| `.set-labels` | Sets comma separated labels to the issue and removes any other labels (e.g. `set-labels l1, 'l2 with spaces'`) |

## Security Model

For information on the Beam CI security model, see https://cwiki.apache.org/confluence/display/BEAM/CI+Security+Model
