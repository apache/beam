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
var service = require("../dist/apache_beam/utils/exp_service");

describe("java expansion service", function () {
  describe("constructor", function () {
    const serviceRunner = new service.JavaExpansionServiceRunner(
      ":sdks:java:fake:runService",
      "VERSION"
    );
    it("should contain the passed gradle target", function () {
      assert.equal(serviceRunner.gradleTarget, ":sdks:java:fake:runService");
    });
    it("should generate the correct jar name", function () {
      assert.equal(serviceRunner.jarName, "beam-sdks-java-fake-VERSION.jar");
    });
  });
});
