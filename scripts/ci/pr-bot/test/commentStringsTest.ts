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
const commentStrings = require("../shared/commentStrings");

describe("commentStrings", function () {
  describe("assignReviewer()", function () {
    it("should not include scrutiny statement when core label is not present", function () {
      const comment = commentStrings.assignReviewer(
        { Java: "reviewer1" },
        { labels: [{ name: "Java" }] }
      );
      assert(!comment.includes("review with scrutiny"));
    });

    it("should include scrutiny statement when core label is present in options.labels", function () {
      const comment = commentStrings.assignReviewer(
        { Java: "reviewer1" },
        { labels: [{ name: "core" }] }
      );
      assert(comment.includes("review with scrutiny"));
    });

    it("should include scrutiny statement when core label is in mapping keys", function () {
      const comment = commentStrings.assignReviewer({ core: "reviewer1" });
      assert(comment.includes("review with scrutiny"));
    });
  });
});
