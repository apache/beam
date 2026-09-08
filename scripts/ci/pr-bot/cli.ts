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

import * as childProcess from "child_process";
import * as https from "https";
import { buildPrHistoryContext } from "./shared/gitHistory";
import {
  GeminiReviewerAdvisor,
  VertexAiClient,
} from "./shared/geminiReviewerAdvisor";
import { assignReviewersWithExpertise } from "./shared/commentStrings";

function printHelp() {
  console.log(`
Beam Review Advisor CLI (Experimental)
Analyzes git commit history and file churn to recommend expert reviewers.

USAGE:
  npm run review-advisor -- [options] [files...]
  node lib/cli.js [options] [files...]

OPTIONS:
  --pr <number>         Fetch touched files and details for a GitHub pull request.
  --local               Inspect uncommitted modified/staged files in local repository.
  --branch <branch>     Inspect files modified relative to a branch (e.g. master).
  --heuristic-only      Run offline recency-decayed familiarity scoring without LLM.
  --project <project>   GCP project ID for Vertex AI (default: gcloud config or apache-beam-testing).
  --json                Output raw JSON result instead of formatted markdown.
  --help, -h            Show this help text.

EXAMPLES:
  # Inspect an open pull request:
  npm run review-advisor -- --pr 39156

  # Inspect specific files offline:
  npm run review-advisor -- --heuristic-only sdks/java/io/kafka/src/main/java/.../KafkaIO.java

  # Inspect local uncommitted changes:
  npm run review-advisor -- --local
`);
}

function fetchJson(url: string): Promise<any> {
  return new Promise((resolve, reject) => {
    https
      .get(
        url,
        {
          headers: {
            "User-Agent": "beam-review-advisor-cli",
            Accept: "application/vnd.github.v3+json",
          },
        },
        (res) => {
          let data = "";
          res.on("data", (chunk) => (data += chunk));
          res.on("end", () => {
            try {
              resolve(JSON.parse(data));
            } catch (err) {
              reject(new Error(`Failed to parse response from ${url}: ${err}`));
            }
          });
        }
      )
      .on("error", reject);
  });
}

async function runCli() {
  const args = process.argv.slice(2);

  if (args.includes("--help") || args.includes("-h")) {
    printHelp();
    return;
  }

  let prNumber: number | undefined;
  let useHeuristicOnly = false;
  let outputJson = false;
  let localDiff = false;
  let baseBranch: string | undefined;
  let customProject: string | undefined;
  const rawFileArgs: string[] = [];

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--pr" && i + 1 < args.length) {
      prNumber = parseInt(args[++i], 10);
    } else if (arg === "--heuristic-only" || arg === "--offline") {
      useHeuristicOnly = true;
    } else if (arg === "--json") {
      outputJson = true;
    } else if (arg === "--local") {
      localDiff = true;
    } else if (arg === "--branch" && i + 1 < args.length) {
      baseBranch = args[++i];
    } else if (arg === "--project" && i + 1 < args.length) {
      customProject = args[++i];
    } else if (!arg.startsWith("--")) {
      rawFileArgs.push(arg);
    }
  }

  let prTitle = "Local Changes";
  let prBody = "";
  let prAuthor = "author";
  let files: Array<{
    filename: string;
    additions: number;
    deletions: number;
    changes: number;
    status: string;
  }> = [];

  if (prNumber) {
    if (!outputJson) {
      console.log(`Fetching PR #${prNumber} details from GitHub...`);
    }
    const prData = await fetchJson(
      `https://api.github.com/repos/apache/beam/pulls/${prNumber}`
    );
    if (!prData || !prData.title) {
      throw new Error(
        `Failed to retrieve PR #${prNumber}: ${JSON.stringify(prData)}`
      );
    }
    prTitle = prData.title;
    prBody = prData.body || "";
    prAuthor = prData.user?.login || "author";

    const filesData = await fetchJson(
      `https://api.github.com/repos/apache/beam/pulls/${prNumber}/files`
    );
    if (Array.isArray(filesData)) {
      files = filesData.map((f: any) => ({
        filename: f.filename,
        additions: f.additions || 0,
        deletions: f.deletions || 0,
        changes: f.changes || (f.additions || 0) + (f.deletions || 0),
        status: f.status || "modified",
      }));
    }
  } else if (localDiff) {
    const diffStat = childProcess
      .execFileSync("git", ["diff", "--stat", "HEAD"], { encoding: "utf8" })
      .trim();
    const nameStatus = childProcess
      .execFileSync("git", ["diff", "--name-status", "HEAD"], {
        encoding: "utf8",
      })
      .trim();

    const lines = nameStatus.split("\n").filter(Boolean);
    for (const line of lines) {
      const parts = line.split(/\s+/);
      if (parts.length >= 2) {
        files.push({
          filename: parts[1],
          additions: 10,
          deletions: 5,
          changes: 15,
          status: parts[0].startsWith("A") ? "added" : "modified",
        });
      }
    }
  } else if (baseBranch) {
    const nameStatus = childProcess
      .execFileSync("git", ["diff", "--name-status", `${baseBranch}...HEAD`], {
        encoding: "utf8",
      })
      .trim();

    const lines = nameStatus.split("\n").filter(Boolean);
    for (const line of lines) {
      const parts = line.split(/\s+/);
      if (parts.length >= 2) {
        files.push({
          filename: parts[1],
          additions: 10,
          deletions: 5,
          changes: 15,
          status: parts[0].startsWith("A") ? "added" : "modified",
        });
      }
    }
  } else if (rawFileArgs.length > 0) {
    files = rawFileArgs.map((f) => ({
      filename: f,
      additions: 25,
      deletions: 5,
      changes: 30,
      status: "modified",
    }));
  } else {
    // Default demo mode
    printHelp();
    console.log("--- RUNNING SAMPLE DEMO ON KAFKAIO ---\n");
    files = [
      {
        filename:
          "sdks/java/io/kafka/src/main/java/org/apache/beam/sdk/io/kafka/KafkaIO.java",
        additions: 80,
        deletions: 15,
        changes: 95,
        status: "modified",
      },
    ];
    prTitle = "KafkaIO: Reader loop stability and dynamic read improvements";
    prBody =
      "Optimize dynamic work rebalancing and consumer watermark tracking.";
    prAuthor = "sampleContributor";
  }

  if (files.length === 0) {
    console.log("No changed files detected.");
    return;
  }

  if (!outputJson) {
    console.log(`Analyzing ${files.length} file(s) across git history...`);
    for (const f of files.slice(0, 5)) {
      console.log(`  - ${f.filename} (+${f.additions}, -${f.deletions})`);
    }
    if (files.length > 5) {
      console.log(`  ... and ${files.length - 5} more file(s)`);
    }
  }

  const prContext = buildPrHistoryContext(
    prNumber || 0,
    prTitle,
    prBody,
    prAuthor,
    files
  );

  const advisor = new GeminiReviewerAdvisor({
    disableLlm: useHeuristicOnly,
    vertexAiConfig: customProject ? { project: customProject } : undefined,
    committerCheck: async (login) =>
      [
        "kennknowles",
        "chamikaramj",
        "jrmccluskey",
        "johnjcasey",
        "damccorm",
        "ahmedabu98",
        "abacn",
      ].includes(login.toLowerCase()),
  });

  const advice = await advisor.adviseReviewers(prContext);

  if (outputJson) {
    console.log(
      JSON.stringify(
        {
          prContext: {
            number: prContext.prNumber,
            title: prContext.title,
            author: prContext.author,
            candidatesCount: prContext.candidates.length,
          },
          advice,
        },
        null,
        2
      )
    );
    return;
  }

  console.log(`\nAdvisor Source: ${advice.source.toUpperCase()}`);
  console.log(`Candidates in git history: ${prContext.candidates.length}`);
  console.log("---------------- Selected Reviewers ----------------");
  for (const reviewer of advice.selectedReviewers) {
    console.log(
      `Reviewer: @${
        reviewer.username
      } [${reviewer.role.toUpperCase()}] (Committer: ${reviewer.isCommitter})`
    );
    console.log(`Expertise: ${reviewer.expertise}`);
    console.log(`Covered files: ${reviewer.coveredFiles.join(", ")}\n`);
  }

  if (advice.alternateReviewers.length > 0) {
    console.log("---------------- Alternate Reviewers ---------------");
    for (const alt of advice.alternateReviewers) {
      console.log(`Backup: @${alt.username} — ${alt.expertise}`);
    }
    console.log();
  }

  console.log("Reasoning: " + advice.reasoning);
  console.log("\n================ Generated GitHub Comment ================\n");
  console.log(assignReviewersWithExpertise(advice));
  console.log("==========================================================");
}

runCli().catch((err) => {
  console.error("Review Advisor error:", err);
  process.exit(1);
});
