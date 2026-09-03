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
import {
  PrHistoryContext,
  CandidateContributor,
  TouchedFileContext,
} from "./gitHistory";

/**
 * Interface representing an individual recommended reviewer.
 */
export interface ReviewerRecommendation {
  readonly username: string;
  readonly role: "primary" | "secondary";
  readonly isCommitter: boolean;
  readonly expertise: string;
  readonly coveredFiles: readonly string[];
}

/**
 * Interface representing an alternate reviewer suggestion.
 */
export interface AlternateReviewer {
  readonly username: string;
  readonly expertise: string;
}

/**
 * Result structure produced by the reviewer advisor.
 */
export interface ReviewerAdviceResult {
  readonly selectedReviewers: readonly ReviewerRecommendation[];
  readonly alternateReviewers: readonly AlternateReviewer[];
  readonly reasoning: string;
  readonly source: "vertex-ai" | "gemini" | "heuristic-fallback";
}

/**
 * Interface for LLM clients that can generate structured JSON.
 */
export interface IGeminiClient {
  generateJson<T>(prompt: string): Promise<T>;
}

/**
 * Configuration options for Vertex AI.
 */
export interface VertexAiConfig {
  readonly project?: string;
  readonly location?: string;
  readonly model?: string;
  readonly token?: string;
}

/**
 * Resolves an OAuth2 access token for Google Cloud.
 * Checks environment variables first, then falls back to gcloud application-default
 * credentials or gcloud auth print-access-token.
 */
export function getGcloudAccessToken(): string {
  if (process.env.VERTEX_TOKEN) {
    return process.env.VERTEX_TOKEN.trim();
  }
  if (process.env.CLOUD_ACCESS_TOKEN) {
    return process.env.CLOUD_ACCESS_TOKEN.trim();
  }
  try {
    const token = childProcess.execFileSync(
      "gcloud",
      ["auth", "application-default", "print-access-token"],
      {
        encoding: "utf8",
        stdio: ["pipe", "pipe", "ignore"],
      }
    );
    if (token && token.trim()) {
      return token.trim();
    }
  } catch {}

  try {
    const token = childProcess.execFileSync(
      "gcloud",
      ["auth", "print-access-token"],
      {
        encoding: "utf8",
        stdio: ["pipe", "pipe", "ignore"],
      }
    );
    if (token && token.trim()) {
      return token.trim();
    }
  } catch {}

  return "";
}

/**
 * Resolves the Google Cloud project ID for Vertex AI requests.
 */
export function getGcloudProject(explicitProject?: string): string {
  if (explicitProject) return explicitProject;
  if (process.env.VERTEX_PROJECT) return process.env.VERTEX_PROJECT;
  if (process.env.GOOGLE_CLOUD_PROJECT) return process.env.GOOGLE_CLOUD_PROJECT;
  if (process.env.CLOUDSDK_CORE_PROJECT)
    return process.env.CLOUDSDK_CORE_PROJECT;
  try {
    const proj = childProcess.execFileSync(
      "gcloud",
      ["config", "get-value", "project"],
      {
        encoding: "utf8",
        stdio: ["pipe", "pipe", "ignore"],
      }
    );
    if (proj && proj.trim() && proj.trim() !== "(unset)") {
      return proj.trim();
    }
  } catch {}
  return "apache-beam-testing";
}

/**
 * Vertex AI LLM client using gcloud OAuth2 authentication.
 */
export class VertexAiClient implements IGeminiClient {
  private readonly project: string;
  private readonly location: string;
  private readonly model: string;
  private readonly token?: string;

  constructor(config: VertexAiConfig = {}) {
    this.project = getGcloudProject(config.project);
    this.location =
      config.location || process.env.VERTEX_LOCATION || "us-central1";
    this.model = config.model || process.env.VERTEX_MODEL || "gemini-2.5-flash";
    this.token = config.token;
  }

  async generateJson<T>(prompt: string): Promise<T> {
    const token = this.token || getGcloudAccessToken();
    if (!token) {
      throw new Error(
        "No Google Cloud access token found. Please authenticate via `gcloud auth application-default login` or set VERTEX_TOKEN."
      );
    }

    const url = `https://${encodeURIComponent(
      this.location
    )}-aiplatform.googleapis.com/v1/projects/${encodeURIComponent(
      this.project
    )}/locations/${encodeURIComponent(
      this.location
    )}/publishers/google/models/${encodeURIComponent(
      this.model
    )}:generateContent`;

    const response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [{ text: prompt }],
          },
        ],
        generationConfig: {
          temperature: 0.1,
          responseMimeType: "application/json",
        },
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(
        `Vertex AI request failed with status ${response.status}: ${errorText}`
      );
    }

    const data: any = await response.json();
    let candidateText = data?.candidates?.[0]?.content?.parts?.[0]?.text;

    if (!candidateText) {
      throw new Error("Empty or invalid candidate response from Vertex AI.");
    }

    candidateText = candidateText.trim();
    if (candidateText.startsWith("```json")) {
      candidateText = candidateText.slice(7);
    } else if (candidateText.startsWith("```")) {
      candidateText = candidateText.slice(3);
    }
    if (candidateText.endsWith("```")) {
      candidateText = candidateText.slice(0, -3);
    }

    return JSON.parse(candidateText.trim()) as T;
  }
}

/**
 * Standard HTTP Gemini client using API key.
 */
export class GeminiClient implements IGeminiClient {
  private readonly apiKey: string;
  private readonly model: string;

  constructor(apiKey: string, model: string = "gemini-2.5-flash") {
    this.apiKey = apiKey;
    this.model = model;
  }

  async generateJson<T>(prompt: string): Promise<T> {
    if (!this.apiKey) {
      throw new Error("API key is not configured.");
    }

    const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      this.model
    )}:generateContent?key=${encodeURIComponent(this.apiKey)}`;

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [{ text: prompt }],
          },
        ],
        generationConfig: {
          temperature: 0.1,
          responseMimeType: "application/json",
        },
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(
        `API request failed with status ${response.status}: ${errorText}`
      );
    }

    const data: any = await response.json();
    let candidateText = data?.candidates?.[0]?.content?.parts?.[0]?.text;

    if (!candidateText) {
      throw new Error("Empty or invalid candidate response.");
    }

    candidateText = candidateText.trim();
    if (candidateText.startsWith("```json")) {
      candidateText = candidateText.slice(7);
    } else if (candidateText.startsWith("```")) {
      candidateText = candidateText.slice(3);
    }
    if (candidateText.endsWith("```")) {
      candidateText = candidateText.slice(0, -3);
    }

    return JSON.parse(candidateText.trim()) as T;
  }
}

/**
 * Configuration options for the Reviewer Advisor.
 */
export interface ReviewerAdvisorOptions {
  readonly llmClient?: IGeminiClient;
  readonly geminiClient?: IGeminiClient;
  readonly vertexAiConfig?: VertexAiConfig;
  readonly disableLlm?: boolean;
  readonly committerCheck?: (username: string) => Promise<boolean>;
  readonly exclusionList?: readonly string[];
  readonly maxReviewers?: number;
}

/**
 * Advisor that analyzes PR git history and selects optimal reviewers using Vertex AI or heuristic fallback.
 */
export class GeminiReviewerAdvisor {
  private readonly client?: IGeminiClient;
  private readonly committerCheck: (username: string) => Promise<boolean>;
  private readonly exclusionList: readonly string[];
  private readonly maxReviewers: number;

  constructor(options: ReviewerAdvisorOptions = {}) {
    this.client = options.disableLlm
      ? undefined
      : options.llmClient ||
        options.geminiClient ||
        new VertexAiClient(options.vertexAiConfig || {});
    this.committerCheck = options.committerCheck ?? (async () => false);
    this.exclusionList = options.exclusionList ?? [];
    this.maxReviewers = options.maxReviewers ?? 2;
  }

  /**
   * Constructs the prompt instructing Gemini on how to select reviewers.
   *
   * @param context Extracted git and PR history.
   * @param committers Map of username to committer status.
   * @returns Detailed prompt string.
   */
  public buildPrompt(
    context: PrHistoryContext,
    committers: Readonly<Record<string, boolean>>
  ): string {
    const fileSummaries = context.touchedFiles.map((file) => {
      const commitSummaries = file.recentCommits
        .slice(0, 5)
        .map((c) => {
          const tag = c.isMechanical ? " [mechanical / formatting bump]" : "";
          return `    - [${c.date}] ${c.authorLogin || c.authorName}${tag}: ${c.subject}`;
        })
        .join("\n");

      return `- File: ${file.path} (+${file.additions}, -${
        file.deletions
      }, changes: ${file.changes}${
        file.isNewFile ? " [NEW FILE]" : ""
      })\n  Recent Commits:\n${commitSummaries || "    (No recent commits)"}`;
    });

    const subsystemSummaries = (context.subsystems || []).map((s) => {
      const top = s.topContributors
        .slice(0, 5)
        .map((tc) => `@${tc.login} (${tc.commitCount} commits)`)
        .join(", ");
      return `- Directory: ${s.directory}\n  Key Contributors (2-3 year history): ${
        top || "(none)"
      }`;
    });

    const candidateSummaries = context.candidates.map((c) => {
      const isCommitter = committers[c.login] ?? false;
      const subsystemDetails =
        c.subsystemCommitCount > 0
          ? `, ${c.subsystemCommitCount} commits across enclosing subsystem(s)`
          : "";
      const expertTag = c.isSubsystemAuthor
        ? " [Subsystem Domain Expert]"
        : "";
      return `- @${c.login} (${c.name})${expertTag}: ${
        c.commitCount
      } commits in touched files${subsystemDetails}, last active ${
        c.lastCommitDate || "recently"
      }, committer=${isCommitter}. Files touched: ${
        c.touchedFilePaths.join(", ") || "(subsystem files)"
      }`;
    });

    const exclusions =
      this.exclusionList.map((e) => `@${e}`).join(", ") || "(none)";

    return `You are the Apache Beam Code Review Assigner.
Your goal is to choose a small, optimal set of expert reviewers for a pull request based on real git history, multi-year subsystem ownership, and technical domain relevance.

Pull Request Context:
- PR Number: #${context.prNumber}
- Title: "${context.title}"
- Author: @${context.author}
- Description: ${context.description || "(No description provided)"}

Files Changed:
${fileSummaries.join("\n\n")}

Enclosing Subsystems & Key Historical Contributors:
${subsystemSummaries.join("\n\n") || "(No subsystem directory history available)"}

Candidate Contributors from Git History:
${candidateSummaries.join("\n") || "(No candidates found in history)"}

Reviewer Exclusions (Do NOT assign):
${exclusions}, and the PR author (@${context.author}).

Assignment Guidelines:
1. REVIEWER SET MINIMIZATION: Choose ideally ONE primary reviewer who can cover the core changes or the most critical subsystem. Only choose two reviewers if the PR touches two completely distinct, major subsystems with no overlapping expert.
2. LONG-TERM DOMAIN EXPERTISE OVER TRANSIENT CHURN: Prioritize authors with significant historical commit volume in the subsystem or affected files over contributors who merely touched the file recently for mechanical maintenance (e.g. Spotless formatting bumps, dependency upgrades, typo fixes, or ErrorProne linter fixes). A contributor with dozens of commits across the subsystem is a vastly superior reviewer than someone with one recent formatting commit.
3. COMMITTING AUTHORITY: Prefer Beam committers when available, as they can merge or authoritatively approve PRs.
4. EXPLICIT EXPERTISE JUSTIFICATION: For each selected reviewer, state their specific technical expertise relevant to this PR in 1-2 concise sentences, mentioning their long-term subsystem ownership or relevant code contributions.
5. ALTERNATES: Suggest 1-2 alternate reviewers in case the primary reviewer is busy or opts out.

Output Format:
Respond ONLY with a JSON object conforming to this schema:
{
  "selectedReviewers": [
    {
      "username": "github_username",
      "role": "primary" or "secondary",
      "isCommitter": boolean,
      "expertise": "Specific technical expertise rationale...",
      "coveredFiles": ["file/path/1", "file/path/2"]
    }
  ],
  "alternateReviewers": [
    {
      "username": "alternate_username",
      "expertise": "Technical rationale..."
    }
  ],
  "reasoning": "Brief explanation of why this set was selected and minimized."
}`;
  }

  /**
   * Deterministic recency-decayed code familiarity fallback when LLM is unavailable.
   *
   * @param context Extracted git history context.
   * @param committers Map of candidate committer status.
   * @returns ReviewerAdviceResult generated via heuristic familiarity.
   */
  public generateHeuristicFallback(
    context: PrHistoryContext,
    committers: Readonly<Record<string, boolean>>
  ): ReviewerAdviceResult {
    const excluded = new Set(
      this.exclusionList
        .concat([context.author])
        .map((u) => u.toLowerCase().trim())
    );

    // Calculate familiarity score per candidate using recency time decay
    const scores = new Map<string, number>();
    const coveredFilesMap = new Map<string, Set<string>>();
    const now = Date.now();

    // 1. File-level score with 365-day recency decay and mechanical commit discounting
    for (const file of context.touchedFiles) {
      const fileWeight = Math.max(1, file.changes);
      for (const commit of file.recentCommits) {
        const login = commit.authorLogin || commit.authorEmail;
        if (!login || excluded.has(login.toLowerCase())) {
          continue;
        }

        const commitTime = new Date(commit.date).getTime();
        const ageDays = Math.max(0, (now - commitTime) / (1000 * 60 * 60 * 24));
        const recencyDecay = 1 / (1 + ageDays / 365);
        const mechanicalFactor = commit.isMechanical ? 0.1 : 1.0;
        const scoreInc = fileWeight * mechanicalFactor * recencyDecay;

        scores.set(login, (scores.get(login) ?? 0) + scoreInc);

        if (!coveredFilesMap.has(login)) {
          coveredFilesMap.set(login, new Set());
        }
        coveredFilesMap.get(login)!.add(file.path);
      }
    }

    // 2. Incorporate multi-year subsystem domain history
    for (const candidate of context.candidates) {
      if (
        candidate.subsystemCommitCount > 0 &&
        !excluded.has(candidate.login.toLowerCase())
      ) {
        let ageDays = 180;
        if (candidate.lastCommitDate) {
          const lastTime = new Date(candidate.lastCommitDate).getTime();
          ageDays = Math.max(0, (now - lastTime) / (1000 * 60 * 60 * 24));
        }
        const recencyDecay = 1 / (1 + ageDays / 365);
        const subsystemScore =
          candidate.subsystemCommitCount * 15 * recencyDecay;

        scores.set(
          candidate.login,
          (scores.get(candidate.login) ?? 0) + subsystemScore
        );
      }
    }

    const sortedCandidates = Array.from(scores.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([login]) => login);

    if (sortedCandidates.length === 0) {
      return {
        selectedReviewers: [],
        alternateReviewers: [],
        reasoning: "No eligible reviewers found in git history.",
        source: "heuristic-fallback",
      };
    }

    const primaryLogin = sortedCandidates[0];
    const primaryCoveredFiles = Array.from(
      coveredFilesMap.get(primaryLogin) ?? []
    );
    const primaryCandidate = context.candidates.find(
      (c) => c.login === primaryLogin
    );

    const primaryExpertise = primaryCandidate?.isSubsystemAuthor
      ? `Subsystem domain expert with ${
          primaryCandidate.subsystemCommitCount
        } commit(s) across enclosing subsystem and ${
          primaryCandidate.commitCount
        } touching modified file(s).`
      : `Active contributor with ${
          primaryCandidate?.commitCount ?? 1
        } commit(s) touching modified files.`;

    const primaryRecommendation: ReviewerRecommendation = {
      username: primaryLogin,
      role: "primary",
      isCommitter: committers[primaryLogin] ?? false,
      expertise: primaryExpertise,
      coveredFiles: Object.freeze(primaryCoveredFiles),
    };

    const alternates: AlternateReviewer[] = [];
    for (let i = 1; i < Math.min(sortedCandidates.length, 3); i++) {
      const altLogin = sortedCandidates[i];
      const altCandidate = context.candidates.find((c) => c.login === altLogin);
      const altExpertise = altCandidate?.isSubsystemAuthor
        ? `Subsystem domain contributor with ${altCandidate.subsystemCommitCount} commit(s) in enclosing subsystem.`
        : `Contributor with ${
            altCandidate?.commitCount ?? 1
          } commit(s) in affected files.`;

      alternates.push({
        username: altLogin,
        expertise: altExpertise,
      });
    }

    return {
      selectedReviewers: Object.freeze([primaryRecommendation]),
      alternateReviewers: Object.freeze(alternates),
      reasoning:
        "Selected top contributor based on multi-year subsystem domain history and recency-decayed file churn.",
      source: "heuristic-fallback",
    };
  }

  /**
   * Evaluates the PR history and produces reviewer recommendations.
   *
   * @param context Extracted git history context.
   * @returns ReviewerAdviceResult.
   */
  public async adviseReviewers(
    context: PrHistoryContext
  ): Promise<ReviewerAdviceResult> {
    const committers: Record<string, boolean> = {};
    for (const candidate of context.candidates) {
      if (candidate.login) {
        committers[candidate.login] = await this.committerCheck(
          candidate.login
        );
      }
    }

    if (!this.client) {
      return this.generateHeuristicFallback(context, committers);
    }

    try {
      const prompt = this.buildPrompt(context, committers);
      const rawResult = await this.client.generateJson<any>(prompt);

      if (
        !rawResult ||
        !Array.isArray(rawResult.selectedReviewers) ||
        rawResult.selectedReviewers.length === 0
      ) {
        console.warn(
          "Gemini returned invalid or empty reviewer set, falling back to heuristics."
        );
        return this.generateHeuristicFallback(context, committers);
      }

      const excluded = new Set(
        this.exclusionList
          .concat([context.author])
          .map((u) => u.toLowerCase().trim())
      );

      const filteredSelected: ReviewerRecommendation[] = [];
      for (const rec of rawResult.selectedReviewers) {
        const uname = (rec.username || "").replace(/^@/, "").trim();
        if (uname && !excluded.has(uname.toLowerCase())) {
          filteredSelected.push({
            username: uname,
            role: rec.role === "secondary" ? "secondary" : "primary",
            isCommitter: committers[uname] ?? false,
            expertise:
              rec.expertise || "Selected for recent subsystem contributions.",
            coveredFiles: Object.freeze(rec.coveredFiles || []),
          });
        }
      }

      if (filteredSelected.length === 0) {
        return this.generateHeuristicFallback(context, committers);
      }

      const filteredAlternates: AlternateReviewer[] = [];
      if (Array.isArray(rawResult.alternateReviewers)) {
        for (const alt of rawResult.alternateReviewers) {
          const uname = (alt.username || "").replace(/^@/, "").trim();
          if (
            uname &&
            !excluded.has(uname.toLowerCase()) &&
            !filteredSelected.some(
              (s) => s.username.toLowerCase() === uname.toLowerCase()
            )
          ) {
            filteredAlternates.push({
              username: uname,
              expertise: alt.expertise || "Contributor to related components.",
            });
          }
        }
      }

      return {
        selectedReviewers: Object.freeze(
          filteredSelected.slice(0, this.maxReviewers)
        ),
        alternateReviewers: Object.freeze(filteredAlternates),
        reasoning:
          rawResult.reasoning ||
          "Selected by model based on git history relevance.",
        source: "vertex-ai",
      };
    } catch (error) {
      console.warn(
        `Error during reviewer advising: ${error}. Using heuristic fallback.`
      );
      return this.generateHeuristicFallback(context, committers);
    }
  }
}
