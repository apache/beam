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
var fs = require("fs");
const { ReviewerConfig } = require("../shared/reviewerConfig");
const configPath = "test-config.yml";
const configContents = `labels:
- name: "Go"
  priority: 2
  reviewers: ["testReviewer1", "testReviewer2"]
  exclusionList: ["testReviewer3"] # These users will never be suggested as reviewers
# I don't know the other areas well enough to assess who the normal committers/contributors who might want to be reviewers are
- name: "Java"
  priority: 3
  reviewers: ["testReviewer3", "testReviewer2"]
  exclusionList: [] # These users will never be suggested as reviewers
- name: "Python"
  priority: 1
  reviewers: ["testReviewer4"]
  exclusionList: [] # These users will never be suggested as reviewers
fallbackReviewers: ["testReviewer5", "testReviewer1", "testReviewer3"] # List of committers to use when no label matches
`;
describe("ReviewerConfig", function () {
  before(function () {
    if (fs.existsSync(configPath)) {
      fs.rmSync(configPath);
    }
    fs.writeFileSync(configPath, configContents);
  });

  after(function () {
    fs.rmSync(configPath);
  });

  describe("getReviewersForLabels()", function () {
    it("should return reviewers only for the highest-priority matching label", function () {
      const config = new ReviewerConfig(configPath);
      const reviewersForLabels = config.getReviewersForLabels(
        [{ name: "Java" }, { name: "Go" }],
        []
      );
      assert.deepEqual(Object.keys(reviewersForLabels), ["Go"]);
      assert(
        reviewersForLabels["Go"].find(
          (reviewer) => reviewer === "testReviewer1"
        ),
        "Return value for Go label should include testReviewer1"
      );
      assert(
        reviewersForLabels["Go"].find(
          (reviewer) => reviewer === "testReviewer2"
        ),
        "Return value for Go label should include testReviewer2"
      );
      assert(
        !reviewersForLabels["Go"].find(
          (reviewer) => reviewer === "testReviewer3"
        ),
        "Return value for Go label should not include testReviewer3"
      );
      assert(
        !reviewersForLabels["Go"].find(
          (reviewer) => reviewer === "testReviewer4"
        ),
        "Return value for Go label should not include testReviewer4"
      );
      assert(
        Object.keys(reviewersForLabels).indexOf("Java") == -1,
        "Lower-priority Java label should not be included when Go matches"
      );
      assert(
        Object.keys(reviewersForLabels).indexOf("Python") == -1,
        "No reviewers should be included for python"
      );
    });

    it("should fall back to next highest-priority label if top priority label has all reviewers excluded", function () {
      const config = new ReviewerConfig(configPath);
      const reviewersForLabels = config.getReviewersForLabels(
        [{ name: "Python" }, { name: "Java" }],
        ["testReviewer4"]
      );
      assert.deepEqual(Object.keys(reviewersForLabels), ["Java"]);
      assert.deepEqual(reviewersForLabels["Java"], [
        "testReviewer3",
        "testReviewer2",
      ]);
    });

    it("should return no entry if a label is not configured", function () {
      const config = new ReviewerConfig(configPath);
      const reviewersForLabels = config.getReviewersForLabels(
        [{ name: "FakeLabel" }],
        []
      );

      assert(
        !("FakeLabel" in reviewersForLabels),
        "FakeLabel should not be included in the returned label map"
      );
    });

    it("should exclude any reviewers who are passed into the exlusionList", function () {
      const config = new ReviewerConfig(configPath);
      const reviewersForLabels = config.getReviewersForLabels(
        [{ name: "Go" }, { name: "Java" }],
        ["testReviewer1"]
      );

      assert(
        !reviewersForLabels["Go"].find(
          (reviewer) => reviewer === "testReviewer1"
        ),
        "testReviewer1 should have been excluded from the result set"
      );
    });
  });

  describe("getReviewersForLabel()", function () {
    it("should return all reviewers configured for a label", function () {
      const config = new ReviewerConfig(configPath);
      const reviewersForGo = config.getReviewersForLabel("Go", []);

      assert(
        reviewersForGo.find((reviewer) => reviewer === "testReviewer1"),
        "Return value for Go label should include testReviewer1"
      );
      assert(
        reviewersForGo.find((reviewer) => reviewer === "testReviewer2"),
        "Return value for Go label should include testReviewer2"
      );
      assert(
        !reviewersForGo.find((reviewer) => reviewer === "testReviewer3"),
        "Return value for Go label should not include testReviewer3"
      );
      assert(
        !reviewersForGo.find((reviewer) => reviewer === "testReviewer4"),
        "Return value for Go label should not include testReviewer4"
      );
    });

    it("should return an empty list if a label is not configured", function () {
      const config = new ReviewerConfig(configPath);
      const reviewersForFakeLabel = config.getReviewersForLabel(
        "FakeLabel",
        []
      );
      assert.equal(0, reviewersForFakeLabel.length);
    });

    it("should exclude any reviewers who are passed into the exlusionList", function () {
      const config = new ReviewerConfig(configPath);
      const reviewersForGo = config.getReviewersForLabel("Go", [
        "testReviewer1",
      ]);

      assert(
        !reviewersForGo.find((reviewer) => reviewer === "testReviewer1"),
        "Return value for Go label should not include testReviewer1"
      );
    });
  });

  describe("getExclusionListForLabel()", function () {
    it("should get the exclusion list configured for a label", function () {
      const config = new ReviewerConfig(configPath);
      const goExclusionList = config.getExclusionListForLabel("Go");

      assert(
        goExclusionList.find((reviewer) => reviewer === "testReviewer3"),
        "Return value for Go label should include testReviewer3"
      );
      assert.equal(1, goExclusionList.length);
    });
  });

  describe("getFallbackReviewers()", function () {
    it("should get the configured falback list", function () {
      const config = new ReviewerConfig(configPath);
      const fallbackReviewers = config.getFallbackReviewers([]);

      assert.equal(3, fallbackReviewers.length);
      assert(
        fallbackReviewers.find((reviewer) => reviewer === "testReviewer5"),
        "Fallback reviewers should include testReviewer5"
      );
      assert(
        fallbackReviewers.find((reviewer) => reviewer === "testReviewer1"),
        "Fallback reviewers should include testReviewer1"
      );
      assert(
        fallbackReviewers.find((reviewer) => reviewer === "testReviewer3"),
        "Fallback reviewers should include testReviewer3"
      );
    });

    it("should not include excluded reviewers", function () {
      const config = new ReviewerConfig(configPath);
      const fallbackReviewers = config.getFallbackReviewers([
        "testReviewer1",
        "testReviewer3",
      ]);

      assert.equal(1, fallbackReviewers.length);
      assert(
        fallbackReviewers.find((reviewer) => reviewer === "testReviewer5"),
        "Fallback reviewers should only include testReviewer5"
      );
    });
  });
});
