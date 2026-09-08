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
import { assignReviewersWithExpertise } from "../shared/commentStrings";
import { ReviewerAdviceResult } from "../shared/geminiReviewerAdvisor";

describe("commentStrings", () => {
  describe("assignReviewersWithExpertise()", () => {
    it("should format primary reviewer and expertise correctly", () => {
      const advice: ReviewerAdviceResult = {
        selectedReviewers: [
          {
            username: "alice",
            role: "primary",
            isCommitter: true,
            expertise:
              "Authored KafkaIO dynamic reads and watermark estimation.",
            coveredFiles: ["sdks/java/io/kafka/KafkaIO.java"],
          },
        ],
        alternateReviewers: [
          {
            username: "bob",
            expertise: "Active contributor to Kafka connector dependencies.",
          },
        ],
        reasoning: "Alice is the most relevant expert on this subsystem.",
        source: "gemini",
      };

      const comment = assignReviewersWithExpertise(advice);

      assert.strictEqual(comment.includes("### 🧭 Reviewer Assignment"), true);
      assert.strictEqual(
        comment.includes("R: @alice (**Primary Reviewer**)"),
        true
      );
      assert.strictEqual(
        comment.includes(
          "*Expertise:* Authored KafkaIO dynamic reads and watermark estimation."
        ),
        true
      );
      assert.strictEqual(comment.includes("Backup expert(s): @bob"), true);
      assert.strictEqual(comment.includes("assign to next reviewer"), true);
      assert.strictEqual(comment.includes("assign based on git history"), true);
    });

    it("should format multiple reviewers when selected", () => {
      const advice: ReviewerAdviceResult = {
        selectedReviewers: [
          {
            username: "alice",
            role: "primary",
            isCommitter: true,
            expertise: "Flink runner engine specialist.",
            coveredFiles: ["runners/flink/Runner.java"],
          },
          {
            username: "charlie",
            role: "secondary",
            isCommitter: false,
            expertise: "KafkaIO maintainer.",
            coveredFiles: ["sdks/java/io/kafka/KafkaIO.java"],
          },
        ],
        alternateReviewers: [],
        reasoning: "Cross-cutting PR touching two distinct subsystems.",
        source: "gemini",
      };

      const comment = assignReviewersWithExpertise(advice);

      assert.strictEqual(
        comment.includes("R: @alice (**Primary Reviewer**)"),
        true
      );
      assert.strictEqual(
        comment.includes("R: @charlie (**Secondary Reviewer**)"),
        true
      );
      assert.strictEqual(
        comment.includes("Flink runner engine specialist."),
        true
      );
      assert.strictEqual(comment.includes("KafkaIO maintainer."), true);
    });
  });
});
