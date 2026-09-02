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
import * as path from "path";

/**
 * Information about a single git commit touching a file.
 */
export interface CommitInfo {
  readonly hash: string;
  readonly authorName: string;
  readonly authorEmail: string;
  readonly authorLogin: string;
  readonly date: string;
  readonly subject: string;
}

/**
 * Contextual history and churn statistics for a touched file.
 */
export interface TouchedFileContext {
  readonly path: string;
  readonly additions: number;
  readonly deletions: number;
  readonly changes: number;
  readonly isNewFile: boolean;
  readonly recentCommits: readonly CommitInfo[];
}

/**
 * Aggregated contributor profile derived from git history.
 */
export interface CandidateContributor {
  readonly login: string;
  readonly name: string;
  readonly email: string;
  readonly commitCount: number;
  readonly lastCommitDate: string;
  readonly touchedFilePaths: readonly string[];
}

/**
 * Full PR context prepared for the LLM reviewer advisor.
 */
export interface PrHistoryContext {
  readonly prNumber: number;
  readonly title: string;
  readonly description: string;
  readonly author: string;
  readonly touchedFiles: readonly TouchedFileContext[];
  readonly candidates: readonly CandidateContributor[];
}

/**
 * Known bot identifiers that should be filtered out from reviewer candidates.
 */
const BOT_IDENTIFIERS: readonly string[] = [
  "dependabot",
  "dependabot[bot]",
  "github-actions",
  "github-actions[bot]",
  "beam-bot",
  "codecov",
  "codecov[bot]",
  "asfgit",
  "spotless",
];

/**
 * File extensions and paths that are trivial or generated and should not dominate reviewer selection.
 */
const LOW_PRIORITY_FILE_PATTERNS: readonly RegExp[] = [
  /\.lock$/,
  /package-lock\.json$/,
  /gradle\.lockfile$/,
  /\.md$/,
  /\.mailmap$/,
  /buildSrc\/.*\.gradle$/,
  /\.gitignore$/,
];

/**
 * Determines whether a commit author represents an automated bot or service account.
 *
 * @param authorName The author display name.
 * @param authorEmail The author email.
 * @param authorLogin The resolved GitHub login if available.
 * @returns True if the author is recognized as an automated bot.
 */
export function isBotAuthor(
  authorName: string,
  authorEmail: string,
  authorLogin: string
): boolean {
  const lowerName = authorName.toLowerCase();
  const lowerEmail = authorEmail.toLowerCase();
  const lowerLogin = authorLogin.toLowerCase();

  for (const bot of BOT_IDENTIFIERS) {
    if (
      lowerName.includes(bot) ||
      lowerEmail.includes(bot) ||
      lowerLogin.includes(bot)
    ) {
      return true;
    }
  }

  if (lowerEmail.includes("noreply@github.com") && lowerName === "github") {
    return true;
  }

  return false;
}

/**
 * Resolves a GitHub username from an email, name, or commit subject.
 *
 * @param authorName The author display name.
 * @param authorEmail The author email address.
 * @param knownLogins Optional mapping of normalized email/name to GitHub usernames.
 * @returns The resolved GitHub login, or empty string if unresolved.
 */
export function resolveAuthorLogin(
  authorName: string,
  authorEmail: string,
  knownLogins: Readonly<Record<string, string>> = {}
): string {
  const lowerEmail = authorEmail.toLowerCase().trim();
  const lowerName = authorName.toLowerCase().trim();

  if (knownLogins[lowerEmail]) {
    return knownLogins[lowerEmail];
  }
  if (knownLogins[lowerName]) {
    return knownLogins[lowerName];
  }

  // GitHub noreply email format: [id+]login@users.noreply.github.com
  const noreplyMatch = lowerEmail.match(
    /^(?:\d+\+)?([a-z0-9](?:[a-z0-9-]*[a-z0-9])?)@users\.noreply\.github\.com$/
  );
  if (noreplyMatch && noreplyMatch[1]) {
    return noreplyMatch[1];
  }

  return "";
}

/**
 * Checks whether a file path is considered a low-priority or generated file.
 *
 * @param filePath Path of the file relative to repository root.
 * @returns True if the file should be given lower weight in reviewer selection.
 */
export function isLowPriorityFile(filePath: string): boolean {
  return LOW_PRIORITY_FILE_PATTERNS.some((pattern) => pattern.test(filePath));
}

let cachedRepoRoot = "";

/**
 * Returns the repository root directory using git rev-parse.
 */
export function getRepoRoot(): string {
  if (!cachedRepoRoot) {
    try {
      const stdout = childProcess.execFileSync(
        "git",
        ["rev-parse", "--show-toplevel"],
        {
          encoding: "utf8",
        }
      );
      cachedRepoRoot = stdout ? stdout.toString().trim() : process.cwd();
    } catch {
      cachedRepoRoot = process.cwd();
    }
  }
  return cachedRepoRoot;
}

/**
 * Extracts recent commits for a specific file or fallback directory using git log.
 *
 * @param filePath Relative path to the file.
 * @param maxCommits Maximum number of recent commits to retrieve.
 * @param workingDirectory Base git directory.
 * @param knownLogins Optional map of author names/emails to GitHub logins.
 * @returns Array of commit information objects.
 */
export function getRecentCommitsForFile(
  filePath: string,
  maxCommits: number = 10,
  workingDirectory?: string,
  knownLogins: Readonly<Record<string, string>> = {}
): readonly CommitInfo[] {
  const execOptions: childProcess.ExecFileSyncOptions = {
    encoding: "utf8",
    maxBuffer: 4 * 1024 * 1024,
    cwd: workingDirectory ?? getRepoRoot(),
  };

  try {
    let rawOutput = childProcess.execFileSync(
      "git",
      [
        "log",
        "-n",
        String(maxCommits),
        "--no-merges",
        "--format=%H%x09%an%x09%ae%x09%as%x09%s",
        "--",
        filePath,
      ],
      execOptions
    );
    let stdout = rawOutput ? rawOutput.toString() : "";

    // If file is new and has no history, fall back to parent directory history
    if (!stdout.trim()) {
      const parentDir = path.dirname(filePath);
      if (parentDir && parentDir !== "." && parentDir !== "/") {
        rawOutput = childProcess.execFileSync(
          "git",
          [
            "log",
            "-n",
            String(maxCommits),
            "--no-merges",
            "--format=%H%x09%an%x09%ae%x09%as%x09%s",
            "--",
            parentDir,
          ],
          execOptions
        );
        stdout = rawOutput ? rawOutput.toString() : "";
      }
    }

    const commits: CommitInfo[] = [];
    const lines = stdout.trim().split("\n");

    for (const line of lines) {
      if (!line.trim()) {
        continue;
      }
      const parts = line.split("\t");
      if (parts.length < 5) {
        continue;
      }
      const [hash, authorName, authorEmail, date, ...subjectParts] = parts;
      const subject = subjectParts.join("\t");
      const login = resolveAuthorLogin(authorName, authorEmail, knownLogins);

      if (!isBotAuthor(authorName, authorEmail, login)) {
        commits.push({
          hash,
          authorName,
          authorEmail,
          authorLogin: login,
          date,
          subject,
        });
      }
    }

    return Object.freeze(commits);
  } catch (error) {
    console.error(`Error reading git log for ${filePath}: ${error}`);
    return Object.freeze([]);
  }
}

/**
 * Aggregates candidate contributors across all touched files, counting their commits and files touched.
 *
 * @param touchedFiles List of touched files with their commit history.
 * @param prAuthor GitHub username of the PR author.
 * @returns Array of candidate contributors sorted by commit count descending.
 */
export function aggregateCandidates(
  touchedFiles: readonly TouchedFileContext[],
  prAuthor: string
): readonly CandidateContributor[] {
  const candidateMap = new Map<
    string,
    {
      login: string;
      name: string;
      email: string;
      commitCount: number;
      lastCommitDate: string;
      touchedFilePaths: Set<string>;
    }
  >();

  const normalizedPrAuthor = prAuthor.toLowerCase().trim();

  for (const file of touchedFiles) {
    for (const commit of file.recentCommits) {
      const candidateKey = commit.authorLogin
        ? commit.authorLogin.toLowerCase()
        : commit.authorEmail.toLowerCase();

      // Skip the PR author
      if (
        commit.authorLogin.toLowerCase() === normalizedPrAuthor ||
        commit.authorName.toLowerCase() === normalizedPrAuthor
      ) {
        continue;
      }

      const existing = candidateMap.get(candidateKey);
      if (existing) {
        existing.commitCount += 1;
        existing.touchedFilePaths.add(file.path);
        if (commit.date > existing.lastCommitDate) {
          existing.lastCommitDate = commit.date;
        }
        if (!existing.login && commit.authorLogin) {
          existing.login = commit.authorLogin;
        }
      } else {
        const touchedPaths = new Set<string>();
        touchedPaths.add(file.path);
        candidateMap.set(candidateKey, {
          login: commit.authorLogin || commit.authorEmail,
          name: commit.authorName,
          email: commit.authorEmail,
          commitCount: 1,
          lastCommitDate: commit.date,
          touchedFilePaths: touchedPaths,
        });
      }
    }
  }

  const result: CandidateContributor[] = [];
  for (const val of candidateMap.values()) {
    result.push({
      login: val.login,
      name: val.name,
      email: val.email,
      commitCount: val.commitCount,
      lastCommitDate: val.lastCommitDate,
      touchedFilePaths: Object.freeze(Array.from(val.touchedFilePaths)),
    });
  }

  // Sort by commit count descending, then by most recent commit date descending
  result.sort((a, b) => {
    if (b.commitCount !== a.commitCount) {
      return b.commitCount - a.commitCount;
    }
    return b.lastCommitDate.localeCompare(a.lastCommitDate);
  });

  return Object.freeze(result);
}

/**
 * Assembles the full PR history context from PR files and git history.
 *
 * @param prNumber GitHub pull request number.
 * @param title PR title.
 * @param description PR description body.
 * @param author PR author username.
 * @param files List of files changed in the PR with additions/deletions.
 * @param options Configuration options.
 * @returns Fully constructed PrHistoryContext.
 */
export function buildPrHistoryContext(
  prNumber: number,
  title: string,
  description: string,
  author: string,
  files: readonly {
    filename: string;
    additions?: number;
    deletions?: number;
    changes?: number;
    status?: string;
  }[],
  options: {
    maxFiles?: number;
    commitsPerFile?: number;
    workingDirectory?: string;
    knownLogins?: Readonly<Record<string, string>>;
  } = {}
): PrHistoryContext {
  const maxFiles = options.maxFiles ?? 15;
  const commitsPerFile = options.commitsPerFile ?? 8;
  const workingDirectory = options.workingDirectory;
  const knownLogins = options.knownLogins ?? {};

  // Sort files so that substantive, high-churn source files appear before trivial/lock files
  const sortedFiles = [...files].sort((a, b) => {
    const aLow = isLowPriorityFile(a.filename) ? 1 : 0;
    const bLow = isLowPriorityFile(b.filename) ? 1 : 0;
    if (aLow !== bLow) {
      return aLow - bLow;
    }
    const aChanges = a.changes ?? (a.additions ?? 0) + (a.deletions ?? 0);
    const bChanges = b.changes ?? (b.additions ?? 0) + (b.deletions ?? 0);
    return bChanges - aChanges;
  });

  const selectedFiles = sortedFiles.slice(0, maxFiles);
  const touchedFileContexts: TouchedFileContext[] = [];

  for (const file of selectedFiles) {
    const additions = file.additions ?? 0;
    const deletions = file.deletions ?? 0;
    const changes = file.changes ?? additions + deletions;
    const isNew = file.status === "added";

    const commits = getRecentCommitsForFile(
      file.filename,
      commitsPerFile,
      workingDirectory,
      knownLogins
    );

    touchedFileContexts.push({
      path: file.filename,
      additions,
      deletions,
      changes,
      isNewFile: isNew,
      recentCommits: commits,
    });
  }

  const frozenFiles = Object.freeze(touchedFileContexts);
  const candidates = aggregateCandidates(frozenFiles, author);

  return {
    prNumber,
    title,
    description,
    author,
    touchedFiles: frozenFiles,
    candidates,
  };
}
