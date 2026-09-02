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

import * as assert from "assert";
import {
  isBotAuthor,
  resolveAuthorLogin,
  isLowPriorityFile,
  getRecentCommitsForFile,
  aggregateCandidates,
  buildPrHistoryContext,
  TouchedFileContext,
} from "../shared/gitHistory";

describe("gitHistory", () => {
  describe("isBotAuthor()", () => {
    it("should identify known bots correctly", () => {
      assert.strictEqual(
        isBotAuthor(
          "dependabot[bot]",
          "dependabot@users.noreply.github.com",
          "dependabot[bot]"
        ),
        true
      );
      assert.strictEqual(
        isBotAuthor("github-actions", "actions@github.com", "github-actions"),
        true
      );
      assert.strictEqual(
        isBotAuthor("beam-bot", "beam-bot@apache.org", "beam-bot"),
        true
      );
      assert.strictEqual(
        isBotAuthor("Codecov", "support@codecov.io", "codecov"),
        true
      );
    });

    it("should not mark human contributors as bots", () => {
      assert.strictEqual(
        isBotAuthor("Kenn Knowles", "klk@google.com", "kennknowles"),
        false
      );
      assert.strictEqual(
        isBotAuthor("Alice", "alice@example.com", "alice"),
        false
      );
    });
  });

  describe("resolveAuthorLogin()", () => {
    it("should extract GitHub username from users.noreply.github.com email", () => {
      assert.strictEqual(
        resolveAuthorLogin("Alice", "123456+alice@users.noreply.github.com"),
        "alice"
      );
      assert.strictEqual(
        resolveAuthorLogin("Bob", "bob-dev@users.noreply.github.com"),
        "bob-dev"
      );
    });

    it("should use knownLogins map if email matches", () => {
      const known = { "klk@google.com": "kennknowles" };
      assert.strictEqual(
        resolveAuthorLogin("Kenn Knowles", "klk@google.com", known),
        "kennknowles"
      );
    });

    it("should return empty string if username cannot be inferred", () => {
      assert.strictEqual(
        resolveAuthorLogin("Unknown", "unknown@example.com"),
        ""
      );
    });
  });

  describe("isLowPriorityFile()", () => {
    it("should recognize lockfiles and documentation as low priority", () => {
      assert.strictEqual(isLowPriorityFile("package-lock.json"), true);
      assert.strictEqual(isLowPriorityFile("gradle.lockfile"), true);
      assert.strictEqual(isLowPriorityFile("README.md"), true);
    });

    it("should recognize source code files as normal priority", () => {
      assert.strictEqual(
        isLowPriorityFile("sdks/java/core/src/main/java/Foo.java"),
        false
      );
      assert.strictEqual(
        isLowPriorityFile("sdks/python/apache_beam/io/kafka.py"),
        false
      );
    });
  });

  describe("getRecentCommitsForFile()", () => {
    it("should read real commits from Beam git repository for KafkaIO.java", () => {
      const kafkaFile =
        "sdks/java/io/kafka/src/main/java/org/apache/beam/sdk/io/kafka/KafkaIO.java";
      const commits = getRecentCommitsForFile(kafkaFile, 3);

      assert.strictEqual(
        commits.length > 0,
        true,
        "Expected commits to be found for KafkaIO.java"
      );
      const firstCommit = commits[0];
      assert.strictEqual(typeof firstCommit.hash, "string");
      assert.strictEqual(firstCommit.hash.length, 40);
      assert.strictEqual(typeof firstCommit.authorName, "string");
      assert.strictEqual(typeof firstCommit.subject, "string");
    });
  });

  describe("aggregateCandidates()", () => {
    it("should rank candidates by commit count and exclude the PR author", () => {
      const touchedFiles: TouchedFileContext[] = [
        {
          path: "fileA.java",
          additions: 10,
          deletions: 2,
          changes: 12,
          isNewFile: false,
          recentCommits: [
            {
              hash: "h1",
              authorName: "Alice",
              authorEmail: "alice@example.com",
              authorLogin: "alice",
              date: "2026-08-01",
              subject: "Commit 1",
            },
            {
              hash: "h2",
              authorName: "Bob",
              authorEmail: "bob@example.com",
              authorLogin: "bob",
              date: "2026-08-02",
              subject: "Commit 2",
            },
            {
              hash: "h3",
              authorName: "Alice",
              authorEmail: "alice@example.com",
              authorLogin: "alice",
              date: "2026-08-03",
              subject: "Commit 3",
            },
          ],
        },
        {
          path: "fileB.java",
          additions: 5,
          deletions: 1,
          changes: 6,
          isNewFile: false,
          recentCommits: [
            {
              hash: "h4",
              authorName: "PrAuthor",
              authorEmail: "prauthor@example.com",
              authorLogin: "prauthor",
              date: "2026-08-04",
              subject: "Commit 4",
            },
            {
              hash: "h5",
              authorName: "Bob",
              authorEmail: "bob@example.com",
              authorLogin: "bob",
              date: "2026-08-05",
              subject: "Commit 5",
            },
          ],
        },
      ];

      const candidates = aggregateCandidates(touchedFiles, "prauthor");

      // PR author must be excluded
      assert.strictEqual(
        candidates.some((c) => c.login === "prauthor"),
        false
      );

      // Alice has 2 commits, Bob has 2 commits
      assert.strictEqual(candidates.length, 2);
      assert.strictEqual(candidates[0].commitCount, 2);
      assert.strictEqual(candidates[1].commitCount, 2);
    });
  });

  describe("buildPrHistoryContext()", () => {
    it("should build structured context from changed files", () => {
      const kafkaFile =
        "sdks/java/io/kafka/src/main/java/org/apache/beam/sdk/io/kafka/KafkaIO.java";
      const context = buildPrHistoryContext(
        12345,
        "Fix KafkaIO poll issue",
        "Resolves deadlock in consumer polling",
        "contributorX",
        [
          {
            filename: kafkaFile,
            additions: 50,
            deletions: 10,
            changes: 60,
            status: "modified",
          },
          {
            filename: "README.md",
            additions: 2,
            deletions: 1,
            changes: 3,
            status: "modified",
          },
        ],
        { maxFiles: 5, commitsPerFile: 3 }
      );

      assert.strictEqual(context.prNumber, 12345);
      assert.strictEqual(context.author, "contributorX");
      // KafkaIO should be prioritized above README.md
      assert.strictEqual(context.touchedFiles[0].path, kafkaFile);
      assert.strictEqual(
        context.touchedFiles[0].recentCommits.length > 0,
        true
      );
    });
  });
});
