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

# PR Bot

This directory holds all the code (except for Actions Workflows) for our PR bot designed to improve the PR experience.
For a list of commands to use when interacting with the bot, see [Commands.md](./Commands.md).
For a design doc explaining the design and implementation, see [Automate Reviewer Assignment](https://docs.google.com/document/d/1FhRPRD6VXkYlLAPhNfZB7y2Yese2FCWBzjx67d3TjBo/edit#)

## PR Bot Logic

The bot consists of three core workflows and a persistent state tracking system:

### 1. New PR Processing (`processNewPrs.ts`)
* Runs periodically on a schedule (every 30 minutes).
* Checks eligible open PRs (skips WIP, drafts, closed, PRs < 20 minutes old, PRs with notifications silenced, or PRs labeled `awaiting triage`).
* Once CI checks pass, assigns reviewers based on configured label mappings in `.github/REVIEWERS.yml` (prioritizing least-recently-assigned reviewers).
* If a non-committer reviewer approves, automatically assigns a committer for final review and merge.
* Sets `Next Action: Reviewers` label.

### 2. PR Updates & Commands (`processPrUpdate.ts`)
* Triggered on PR pushes (`synchronize`) and comments (`issue_comment: created`).
* Shifts attention back to reviewers (`Next Action: Reviewers`) when author pushes new commits or posts comments.
* Removes `slow-review` label upon receiving a comment from a non-author reviewer.
* Processes commands like `assign to next reviewer`, `waiting on author`, `stop reviewer notifications`, `assign set of reviewers`, and `remind me after tests pass`.

### 3. Reviewer Reminders & Stale PRs (`findPrsNeedingAttention.ts`)
* Runs daily to identify PRs needing action.
* Flags PRs awaiting reviewer response as `slow-review` if inactive for ≥ 7 days (or ≥ 2 weekdays without comments).
* If still no response after 2 more weekdays, reassigns to new reviewers, removes `slow-review`, and adds `reassigned-reviewers`.
* **Stale PR Cutoff**: If a PR has both `reassigned-reviewers` and `Next Action: Reviewers` labels and review started > 60 days ago, it stops reviewer assignment loops and adds `awaiting triage`. PRs labeled `awaiting triage` are skipped.
* **Stale State Cleanup**: Cleans up the oldest 100 state files for PRs that are no longer open to incrementally prune closed PR metadata from the state branch.

### 4. Persistent State (`PersistentState`)
* Stores PR review progress and label assignment rotations on the `pr-bot-state` Git branch under `state/pr-state/pr-<number>.json` and `state/reviewers-for-label-<label>.json`.

## Build/Test

To build, run:

```
npm install
npm run build
```

To run the tests:

```
npm test
```

Before checking in code, run prettier on it:

```
npm run format
```