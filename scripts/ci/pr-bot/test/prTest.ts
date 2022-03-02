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
const { Pr } = require("../shared/pr");
describe("Pr", function () {
  describe("getLabelForReviewer()", function () {
    it("should return the label a reviewer is assigned to", function () {
      let testPr = new Pr({});
      testPr.reviewersAssignedForLabels = {
        Go: "testReviewer1",
        Java: "testReviewer2",
        Python: "testReviewer3",
      };
      assert.equal("Go", testPr.getLabelForReviewer("testReviewer1"));
      assert.equal("Java", testPr.getLabelForReviewer("testReviewer2"));
      assert.equal("Python", testPr.getLabelForReviewer("testReviewer3"));
    });

    it("should return an empty string when a reviewer is not assigned", function () {
      let testPr = new Pr({});
      testPr.reviewersAssignedForLabels = {
        Go: "testReviewer1",
        Java: "testReviewer2",
        Python: "testReviewer3",
      };
      assert.equal("", testPr.getLabelForReviewer("testReviewer4"));
    });
  });
});
