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
  GeminiReviewerAdvisor,
  IGeminiClient,
} from "../shared/geminiReviewerAdvisor";
import { PrHistoryContext } from "../shared/gitHistory";

/**
 * Concrete test client that delivers a preconfigured JSON payload or throws an error.
 */
class StaticJsonGeminiClient implements IGeminiClient {
  private readonly response: any;
  private readonly shouldThrow: boolean;
  public lastPrompt: string = "";

  constructor(response: any, shouldThrow: boolean = false) {
    this.response = response;
    this.shouldThrow = shouldThrow;
  }

  async generateJson<T>(prompt: string): Promise<T> {
    this.lastPrompt = prompt;
    if (this.shouldThrow) {
      throw new Error("Network timeout or invalid API key");
    }
    return this.response as T;
  }
}

describe("GeminiReviewerAdvisor", () => {
  const sampleContext: PrHistoryContext = {
    prNumber: 9999,
    title: "KafkaIO: Fix consumer poll deadlock",
    description: "Fixes an issue where poll() blocks indefinitely",
    author: "contributorA",
    touchedFiles: [
      {
        path: "sdks/java/io/kafka/KafkaIO.java",
        additions: 120,
        deletions: 15,
        changes: 135,
        isNewFile: false,
        recentCommits: [
          {
            hash: "h1",
            authorName: "Alice",
            authorEmail: "alice@example.com",
            authorLogin: "alice",
            date: "2026-08-15",
            subject: "Refactor KafkaIO reader watermark estimation",
            isMechanical: false,
          },
          {
            hash: "h2",
            authorName: "Bob",
            authorEmail: "bob@example.com",
            authorLogin: "bob",
            date: "2026-07-10",
            subject: "Bump kafka clients version",
            isMechanical: true,
          },
        ],
      },
    ],
    subsystems: [
      {
        directory: "sdks/java/io/kafka",
        topContributors: [
          {
            login: "alice",
            name: "Alice",
            email: "alice@example.com",
            commitCount: 20,
            directory: "sdks/java/io/kafka",
          },
        ],
      },
    ],
    candidates: [
      {
        login: "alice",
        name: "Alice",
        email: "alice@example.com",
        commitCount: 5,
        lastCommitDate: "2026-08-15",
        touchedFilePaths: ["sdks/java/io/kafka/KafkaIO.java"],
        subsystemCommitCount: 20,
        isSubsystemAuthor: true,
      },
      {
        login: "bob",
        name: "Bob",
        email: "bob@example.com",
        commitCount: 2,
        lastCommitDate: "2026-07-10",
        touchedFilePaths: ["sdks/java/io/kafka/KafkaIO.java"],
        subsystemCommitCount: 2,
        isSubsystemAuthor: false,
      },
    ],
  };

  describe("buildPrompt()", () => {
    it("should construct prompt containing PR details, file changes, and instructions", () => {
      const advisor = new GeminiReviewerAdvisor({
        exclusionList: ["busyReviewer"],
      });
      const prompt = advisor.buildPrompt(sampleContext, {
        alice: true,
        bob: false,
      });

      assert.strictEqual(prompt.includes("PR Number: #9999"), true);
      assert.strictEqual(
        prompt.includes("KafkaIO: Fix consumer poll deadlock"),
        true
      );
      assert.strictEqual(prompt.includes("KafkaIO.java"), true);
      assert.strictEqual(prompt.includes("@alice"), true);
      assert.strictEqual(prompt.includes("committer=true"), true);
      assert.strictEqual(prompt.includes("@busyReviewer"), true);
      assert.strictEqual(prompt.includes("@contributorA"), true);
      assert.strictEqual(prompt.includes("REVIEWER SET MINIMIZATION"), true);
    });
  });

  describe("generateHeuristicFallback()", () => {
    it("should select the most active recent contributor using recency-decayed scoring", () => {
      const advisor = new GeminiReviewerAdvisor();
      const result = advisor.generateHeuristicFallback(sampleContext, {
        alice: true,
        bob: false,
      });

      assert.strictEqual(result.source, "heuristic-fallback");
      assert.strictEqual(result.selectedReviewers.length, 1);
      assert.strictEqual(result.selectedReviewers[0].username, "alice");
      assert.strictEqual(result.selectedReviewers[0].role, "primary");
      assert.strictEqual(result.selectedReviewers[0].isCommitter, true);
      assert.strictEqual(result.alternateReviewers.length > 0, true);
      assert.strictEqual(result.alternateReviewers[0].username, "bob");
    });

    it("should respect exclusion list in heuristic fallback", () => {
      const advisor = new GeminiReviewerAdvisor({
        exclusionList: ["alice"],
      });
      const result = advisor.generateHeuristicFallback(sampleContext, {
        alice: true,
        bob: false,
      });

      assert.strictEqual(result.selectedReviewers.length, 1);
      assert.strictEqual(result.selectedReviewers[0].username, "bob");
    });
  });

  describe("adviseReviewers() with LLM client", () => {
    it("should parse and return structured recommendations from LLM", async () => {
      const fakeGeminiResponse = {
        selectedReviewers: [
          {
            username: "alice",
            role: "primary",
            isCommitter: true,
            expertise:
              "Authored core KafkaIO watermark estimation logic; directly familiar with reader loop.",
            coveredFiles: ["sdks/java/io/kafka/KafkaIO.java"],
          },
        ],
        alternateReviewers: [
          {
            username: "bob",
            expertise:
              "Updated Kafka clients and familiar with configuration dependencies.",
          },
        ],
        reasoning:
          "Alice's previous changes directly touched the watermark estimation loop being fixed here.",
      };

      const testClient = new StaticJsonGeminiClient(fakeGeminiResponse);
      const advisor = new GeminiReviewerAdvisor({
        geminiClient: testClient,
        committerCheck: async (login) => login === "alice",
      });

      const result = await advisor.adviseReviewers(sampleContext);

      assert.strictEqual(result.source, "vertex-ai");
      assert.strictEqual(result.selectedReviewers.length, 1);
      assert.strictEqual(result.selectedReviewers[0].username, "alice");
      assert.strictEqual(result.selectedReviewers[0].role, "primary");
      assert.strictEqual(result.selectedReviewers[0].isCommitter, true);
      assert.strictEqual(
        result.selectedReviewers[0].expertise,
        "Authored core KafkaIO watermark estimation logic; directly familiar with reader loop."
      );
      assert.strictEqual(result.alternateReviewers.length, 1);
      assert.strictEqual(result.alternateReviewers[0].username, "bob");
      assert.strictEqual(
        result.reasoning.includes("watermark estimation loop"),
        true
      );
    });

    it("should run offline heuristic when disableLlm is set to true", async () => {
      const advisor = new GeminiReviewerAdvisor({
        disableLlm: true,
        committerCheck: async (login) => login === "alice",
      });

      const result = await advisor.adviseReviewers(sampleContext);

      assert.strictEqual(result.source, "heuristic-fallback");
      assert.strictEqual(result.selectedReviewers.length, 1);
      assert.strictEqual(result.selectedReviewers[0].username, "alice");
    });

    it("should automatically fall back to heuristics if the LLM call fails", async () => {
      const failingClient = new StaticJsonGeminiClient(null, true);
      const advisor = new GeminiReviewerAdvisor({
        geminiClient: failingClient,
        committerCheck: async (login) => login === "alice",
      });

      const result = await advisor.adviseReviewers(sampleContext);

      assert.strictEqual(result.source, "heuristic-fallback");
      assert.strictEqual(result.selectedReviewers.length, 1);
      assert.strictEqual(result.selectedReviewers[0].username, "alice");
    });
  });
});
