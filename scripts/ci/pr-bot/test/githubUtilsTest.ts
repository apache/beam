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

var assert = require("assert");
const { hasLabel } = require("../shared/githubUtils");

describe("githubUtils", function () {
  describe("hasLabel()", function () {
    it("should return true when label object matches exactly", function () {
      const pull = {
        labels: [{ name: "awaiting triage" }, { name: "go" }],
      };
      assert.equal(hasLabel(pull, "awaiting triage"), true);
    });

    it("should return true when label object matches case-insensitively", function () {
      const pull = {
        labels: [{ name: "Awaiting Triage" }],
      };
      assert.equal(hasLabel(pull, "awaiting triage"), true);
      assert.equal(hasLabel(pull, "AWAITING TRIAGE"), true);
    });

    it("should return true when label is a string", function () {
      const pull = {
        labels: ["reassigned-reviewers", "go"],
      };
      assert.equal(hasLabel(pull, "reassigned-reviewers"), true);
      assert.equal(hasLabel(pull, "REASSIGNED-REVIEWERS"), true);
    });

    it("should return false when label is not present", function () {
      const pull = {
        labels: [{ name: "go" }, { name: "python" }],
      };
      assert.equal(hasLabel(pull, "awaiting triage"), false);
      assert.equal(hasLabel(pull, "reassigned-reviewers"), false);
    });

    it("should return false when pull or labels are empty or missing", function () {
      assert.equal(hasLabel({}, "awaiting triage"), false);
      assert.equal(hasLabel({ labels: [] }, "awaiting triage"), false);
      assert.equal(hasLabel(null, "awaiting triage"), false);
    });
  });
});

export {};
