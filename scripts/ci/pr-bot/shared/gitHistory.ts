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
  readonly isMechanical: boolean;
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
 * Contributor summary for an enclosing subsystem or directory.
 */
export interface SubsystemContributor {
  readonly login: string;
  readonly name: string;
  readonly email: string;
  readonly commitCount: number;
  readonly directory: string;
}

/**
 * Summary of history and key contributors for a subsystem directory.
 */
export interface SubsystemHistoryContext {
  readonly directory: string;
  readonly topContributors: readonly SubsystemContributor[];
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
  readonly subsystemCommitCount: number;
  readonly isSubsystemAuthor: boolean;
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
  readonly subsystems: readonly SubsystemHistoryContext[];
  readonly candidates: readonly CandidateContributor[];
}

/**
 * Known contributor emails to GitHub logins mapping.
 */
export const DEFAULT_KNOWN_LOGINS: Readonly<Record<string, string>> = Object.freeze({
  "yathu@google.com": "Abacn",
  "huuyyi@gmail.com": "Abacn",
  "relax@google.com": "reuvenlax",
  "radoslaws@google.com": "radoslaws",
  "dannymccormick@google.com": "damccorm",
  "klk@google.com": "kennknowles",
  "kenn@apache.org": "kennknowles",
  "robertwb@gmail.com": "robertwb",
  "robertwb@google.com": "robertwb",
  "ahmedabualsaud@google.com": "ahmedabu98",
  "clairem@spotify.com": "clairemcginty",
  "michel@davit.fr": "mdavit",
  "huxiangqian@gmail.com": "liferoad",
  "shunping@google.com": "shunping",
  "derrickaw@google.com": "derrickaw",
  "chamikaramj@gmail.com": "chamikaramj",
  "johnjcasey@google.com": "johnjcasey",
  "jrmccluskey@users.noreply.github.com": "jrmccluskey",
  "lostluck@users.noreply.github.com": "lostluck",
  "elialiu760317@outlook.com": "Eliaaazzz",
});

/**
 * Commit subject patterns representing mechanical maintenance, automated bumps, or linters.
 */
const MECHANICAL_COMMIT_PATTERNS: readonly RegExp[] = [
  /\bspotless\b/i,
  /\berror\s*prone\b/i,
  /\bcheckstyle\b/i,
  /^bump\s+/i,
  /^upgrade\s+(?:spotless|gradle|dependencies|wrapper|errorprone)\b/i,
  /^\w+:\s*bump\s+/i,
  /\bformat(?:ting)?\s+(?:code|files?|java|python|go)\b/i,
  /\btypo(?:s)?\b/i,
];

/**
 * Determines whether a commit subject represents mechanical churn rather than domain logic.
 *
 * @param subject The git commit subject line.
 * @returns True if the commit is mechanical/maintenance.
 */
export function isMechanicalCommit(subject: string): boolean {
  return MECHANICAL_COMMIT_PATTERNS.some((pattern) => pattern.test(subject));
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
  if (DEFAULT_KNOWN_LOGINS[lowerEmail]) {
    return DEFAULT_KNOWN_LOGINS[lowerEmail];
  }
  if (knownLogins[lowerName]) {
    return knownLogins[lowerName];
  }
  if (DEFAULT_KNOWN_LOGINS[lowerName]) {
    return DEFAULT_KNOWN_LOGINS[lowerName];
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
  maxCommits: number = 30,
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
      const isMechanical = isMechanicalCommit(subject);

      if (!isBotAuthor(authorName, authorEmail, login)) {
        commits.push({
          hash,
          authorName,
          authorEmail,
          authorLogin: login,
          date,
          subject,
          isMechanical,
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
 * Extracts enclosing subsystem or component directories for the touched files.
 *
 * @param filePaths Relative paths of touched files.
 * @returns Deduplicated list of directory paths.
 */
export function getSubsystemDirectories(
  filePaths: readonly string[]
): readonly string[] {
  const dirs = new Set<string>();

  for (const filePath of filePaths) {
    if (isLowPriorityFile(filePath)) {
      continue;
    }
    const dir = path.dirname(filePath);
    if (dir && dir !== "." && dir !== "/") {
      dirs.add(dir);
    }
  }

  return Object.freeze(Array.from(dirs));
}

/**
 * Fetches multi-year contributor statistics for a list of subsystem directories.
 *
 * @param directories Array of relative directory paths.
 * @param workingDirectory Base git directory.
 * @param knownLogins Optional map of author names/emails to GitHub logins.
 * @returns Array of subsystem history contexts.
 */
export function getSubsystemHistory(
  directories: readonly string[],
  workingDirectory?: string,
  knownLogins: Readonly<Record<string, string>> = {}
): readonly SubsystemHistoryContext[] {
  const execOptions: childProcess.ExecFileSyncOptions = {
    encoding: "utf8",
    maxBuffer: 4 * 1024 * 1024,
    cwd: workingDirectory ?? getRepoRoot(),
  };

  const results: SubsystemHistoryContext[] = [];

  for (const dir of directories) {
    try {
      let rawOutput = childProcess.execFileSync(
        "git",
        [
          "shortlog",
          "-sne",
          "--no-merges",
          "--since=2 years ago",
          "HEAD",
          "--",
          dir,
        ],
        execOptions
      );
      let stdout = rawOutput ? rawOutput.toString().trim() : "";

      if (!stdout || stdout.split("\n").length < 3) {
        const allTimeOutput = childProcess.execFileSync(
          "git",
          ["shortlog", "-sne", "--no-merges", "HEAD", "--", dir],
          execOptions
        );
        const allTimeStr = allTimeOutput ? allTimeOutput.toString().trim() : "";
        if (allTimeStr) {
          stdout = allTimeStr;
        } else {
          const srcIdx = dir.indexOf("/src/");
          if (srcIdx !== -1) {
            const moduleDir = dir.substring(0, srcIdx);
            if (moduleDir && moduleDir !== dir) {
              const moduleOutput = childProcess.execFileSync(
                "git",
                [
                  "shortlog",
                  "-sne",
                  "--no-merges",
                  "--since=2 years ago",
                  "HEAD",
                  "--",
                  moduleDir,
                ],
                execOptions
              );
              stdout = moduleOutput ? moduleOutput.toString().trim() : "";
            }
          }
        }
      }

      if (!stdout) {
        continue;
      }

      const contributors: SubsystemContributor[] = [];
      const lines = stdout.split("\n");

      for (const line of lines) {
        const match = line.match(/^\s*(\d+)\s+([^<]+?)\s*<([^>]+)>/);
        if (!match) {
          continue;
        }
        const commitCount = parseInt(match[1], 10);
        const name = match[2].trim();
        const email = match[3].trim();
        const login = resolveAuthorLogin(name, email, knownLogins);

        if (!isBotAuthor(name, email, login)) {
          contributors.push({
            login: login || email,
            name,
            email,
            commitCount,
            directory: dir,
          });
        }
      }

      results.push({
        directory: dir,
        topContributors: Object.freeze(contributors),
      });
    } catch (error) {
      console.warn(`Error reading subsystem history for ${dir}: ${error}`);
    }
  }

  return Object.freeze(results);
}

/**
 * Aggregates candidate contributors across touched files and enclosing subsystems.
 *
 * @param touchedFiles List of touched files with their commit history.
 * @param prAuthor GitHub username of the PR author.
 * @param subsystemContributors Optional list of subsystem contributors.
 * @returns Array of candidate contributors sorted by domain relevance.
 */
export function aggregateCandidates(
  touchedFiles: readonly TouchedFileContext[],
  prAuthor: string,
  subsystemContributors: readonly SubsystemContributor[] = []
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
      subsystemCommitCount: number;
    }
  >();

  const normalizedPrAuthor = prAuthor.toLowerCase().trim();

  // 1. Process commits touching specific files
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
          subsystemCommitCount: 0,
        });
      }
    }
  }

  // 2. Incorporate subsystem / directory contributors
  let totalSubsystemCommits = 0;
  for (const sub of subsystemContributors) {
    totalSubsystemCommits += sub.commitCount;
    const candidateKey = sub.login
      ? sub.login.toLowerCase()
      : sub.email.toLowerCase();

    if (
      sub.login.toLowerCase() === normalizedPrAuthor ||
      sub.name.toLowerCase() === normalizedPrAuthor
    ) {
      continue;
    }

    const existing = candidateMap.get(candidateKey);
    if (existing) {
      existing.subsystemCommitCount += sub.commitCount;
      if (!existing.login && sub.login) {
        existing.login = sub.login;
      }
    } else {
      candidateMap.set(candidateKey, {
        login: sub.login || sub.email,
        name: sub.name,
        email: sub.email,
        commitCount: 0,
        lastCommitDate: "",
        touchedFilePaths: new Set(),
        subsystemCommitCount: sub.commitCount,
      });
    }
  }

  const result: CandidateContributor[] = [];
  for (const val of candidateMap.values()) {
    const isSubsystemAuthor =
      val.subsystemCommitCount >= 5 ||
      (totalSubsystemCommits > 0 &&
        val.subsystemCommitCount / totalSubsystemCommits >= 0.15);

    result.push({
      login: val.login,
      name: val.name,
      email: val.email,
      commitCount: val.commitCount,
      lastCommitDate: val.lastCommitDate,
      touchedFilePaths: Object.freeze(Array.from(val.touchedFilePaths)),
      subsystemCommitCount: val.subsystemCommitCount,
      isSubsystemAuthor,
    });
  }

  // Sort candidates by combined effective weight:
  // (fileCommitCount * 3 + subsystemCommitCount * 1)
  result.sort((a, b) => {
    const weightA = a.commitCount * 3 + a.subsystemCommitCount * 1;
    const weightB = b.commitCount * 3 + b.subsystemCommitCount * 1;
    if (weightB !== weightA) {
      return weightB - weightA;
    }
    if (b.subsystemCommitCount !== a.subsystemCommitCount) {
      return b.subsystemCommitCount - a.subsystemCommitCount;
    }
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
  const commitsPerFile = options.commitsPerFile ?? 30;
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

  // Extract subsystem history for enclosing directories
  const subsystemDirs = getSubsystemDirectories(
    selectedFiles.map((f) => f.filename)
  );
  const subsystems = getSubsystemHistory(
    subsystemDirs,
    workingDirectory,
    knownLogins
  );
  const allSubsystemContributors: SubsystemContributor[] = [];
  for (const sub of subsystems) {
    for (const c of sub.topContributors) {
      allSubsystemContributors.push(c);
    }
  }

  const candidates = aggregateCandidates(
    frozenFiles,
    author,
    Object.freeze(allSubsystemContributors)
  );

  return {
    prNumber,
    title,
    description,
    author,
    touchedFiles: frozenFiles,
    subsystems,
    candidates,
  };
}
