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

const exec = require("@actions/exec");
const fs = require("fs");
const path = require("path");
const { Pr } = require("./pr");
const { ReviewersForLabel } = require("./reviewersForLabel");
const { BOT_NAME } = require("./constants");

function getPrFileName(prNumber) {
  return `pr-${prNumber}.json`.toLowerCase();
}

function getReviewersForLabelFileName(label) {
  return `reviewers-for-label-${label}.json`.toLowerCase();
}

async function commitStateToRepo() {
  try {
    await exec.exec("git pull origin pr-bot-state");
  } catch (err) {
    console.log(
      `Unable to get most recent repo contents, commit may fail: ${err}`
    );
  }
  // Print changes for observability
  await exec.exec("git status", [], { ignoreReturnCode: true });
  await exec.exec("git add state/*");
  const changes = await exec.exec(
    "git diff --quiet --cached origin/pr-bot-state state",
    [],
    { ignoreReturnCode: true }
  );
  if (changes == 1) {
    await exec.exec(`git commit -m "Updating config from bot" --allow-empty`);
    await exec.exec("git push origin pr-bot-state");
  } else {
    console.log(
      "Skipping updating state branch since there are no changes to commit"
    );
  }
}

export class PersistentState {
  private switchedBranch = false;

  // Returns a Pr object representing the current saved state of the pr.
  async getPrState(prNumber: number): Promise<typeof Pr> {
    var fileName = getPrFileName(prNumber);
    return new Pr(await this.getState(fileName, "state/pr-state"));
  }

  // Writes a Pr object representing the current saved state of the pr to persistent storage.
  async writePrState(prNumber: number, newState: any) {
    var fileName = getPrFileName(prNumber);
    await this.writeState(fileName, "state/pr-state", new Pr(newState));
  }

  // Returns a ReviewersForLabel object representing the current saved state of which reviewers have reviewed recently.
  async getReviewersForLabelState(
    label: string
  ): Promise<typeof ReviewersForLabel> {
    var fileName = getReviewersForLabelFileName(label);
    return new ReviewersForLabel(label, await this.getState(fileName, "state"));
  }

  // Writes a ReviewersForLabel object representing the current saved state of which reviewers have reviewed recently.
  async writeReviewersForLabelState(label: string, newState: any) {
    var fileName = getReviewersForLabelFileName(label);
    await this.writeState(
      fileName,
      "state",
      new ReviewersForLabel(label, newState)
    );
  }

  private async getState(fileName, baseDirectory) {
    await this.ensureCorrectBranch();
    fileName = path.join(baseDirectory, fileName);
    if (!fs.existsSync(fileName)) {
      return null;
    }
    return JSON.parse(fs.readFileSync(fileName, { encoding: "utf-8" }));
  }

  private async writeState(fileName, baseDirectory, state) {
    await this.ensureCorrectBranch();
    fileName = path.join(baseDirectory, fileName);
    if (!fs.existsSync(baseDirectory)) {
      fs.mkdirSync(baseDirectory, { recursive: true });
    }
    fs.writeFileSync(fileName, JSON.stringify(state, null, 2), {
      encoding: "utf-8",
    });
    await commitStateToRepo();
  }

  private async ensureCorrectBranch() {
    if (this.switchedBranch) {
      return;
    }
    console.log(
      "Switching to branch pr-bot-state for reading/storing persistent state between runs"
    );
    try {
      await exec.exec(`git config user.name ${BOT_NAME}`);
      await exec.exec(`git config user.email ${BOT_NAME}@github.com`);
      await exec.exec("git config pull.rebase false");
      await exec.exec("git fetch origin pr-bot-state");
      await exec.exec("git checkout pr-bot-state");
    } catch {
      console.log(
        "Couldnt find branch pr-bot-state in origin, trying to create it"
      );
      await exec.exec("git checkout -b pr-bot-state");
      // Ensure that if we've created or just checked out the branch that we can also push to it
      await exec.exec("git push origin pr-bot-state");
    }
    this.switchedBranch = true;
  }
}
