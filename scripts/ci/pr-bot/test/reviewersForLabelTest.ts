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
const { ReviewersForLabel } = require("../shared/reviewersForLabel");
describe("ReviewersForLabel", function () {
  describe("assignNextReviewer()", function () {
    it("should repeatedly assign the reviewer who reviewed least recently", function () {
      const dateOfLastReviewAssignment = {
        testReviewer1: 1,
        testReviewer2: 3,
        testReviewer3: 4,
        testReviewer4: 2,
      };
      let reviewersForGo = new ReviewersForLabel("Go", {
        dateOfLastReviewAssignment: dateOfLastReviewAssignment,
      });

      assert.equal(
        "testReviewer1",
        reviewersForGo.assignNextReviewer([
          "testReviewer1",
          "testReviewer2",
          "testReviewer3",
          "testReviewer4",
        ])
      );
      assert.equal(
        "testReviewer4",
        reviewersForGo.assignNextReviewer([
          "testReviewer1",
          "testReviewer2",
          "testReviewer3",
          "testReviewer4",
        ])
      );
      assert.equal(
        "testReviewer2",
        reviewersForGo.assignNextReviewer([
          "testReviewer1",
          "testReviewer2",
          "testReviewer3",
          "testReviewer4",
        ])
      );
      assert.equal(
        "testReviewer3",
        reviewersForGo.assignNextReviewer([
          "testReviewer1",
          "testReviewer2",
          "testReviewer3",
          "testReviewer4",
        ])
      );
      assert.equal(
        "testReviewer1",
        reviewersForGo.assignNextReviewer([
          "testReviewer1",
          "testReviewer2",
          "testReviewer3",
          "testReviewer4",
        ])
      );
    });

    it("should assign a reviewer who hasnt reviewed before", function () {
      const dateOfLastReviewAssignment = {
        testReviewer1: 1,
        testReviewer2: 2,
        testReviewer3: 3,
        testReviewer4: 4,
      };
      let reviewersForGo = new ReviewersForLabel("Go", {
        dateOfLastReviewAssignment: dateOfLastReviewAssignment,
      });

      assert.equal(
        "testReviewer5",
        reviewersForGo.assignNextReviewer([
          "testReviewer1",
          "testReviewer2",
          "testReviewer3",
          "testReviewer4",
          "testReviewer5",
        ])
      );
    });

    it("should only assign reviewers in the availableReviewers list", function () {
      const dateOfLastReviewAssignment = {
        testReviewer1: 1,
        testReviewer2: 2,
        testReviewer3: 3,
        testReviewer4: 4,
      };
      let reviewersForGo = new ReviewersForLabel("Go", {
        dateOfLastReviewAssignment: dateOfLastReviewAssignment,
      });

      assert.equal(
        "testReviewer2",
        reviewersForGo.assignNextReviewer(["testReviewer4", "testReviewer2"])
      );
    });

    it("should throw if no reviewer available", function () {
      const dateOfLastReviewAssignment = {
        testReviewer1: 1,
        testReviewer2: 2,
        testReviewer3: 3,
        testReviewer4: 4,
      };
      let reviewersForGo = new ReviewersForLabel("Go", {
        dateOfLastReviewAssignment: dateOfLastReviewAssignment,
      });

      assert.throws(() => reviewersForGo.assignNextReviewer([]));
    });
  });
});
