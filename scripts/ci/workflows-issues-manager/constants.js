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

const DEFAULT_K = 5;

//Overrrides DEFAULT_K as the number of successful runs the workflow needs to have to be stable.
//Works with workflow.name and workflow.id as key
const MIN_RUNS = {
  //Examples:
  // "Build python source distribution and wheels": 10,
  // 10535798: 15,
};

const ISSUES_MANAGER_TAG = "ISSUES_MANAGER";
const ISSUES_MANAGER_LABEL = 'issues-manager';
const ISSUE_STATUS = {
  CREATED: "CREATED",
  EXISTENT: "EXISTENT",
  SKIPPED: "SKIPPED",
  CLOSED: "CLOSED"
};

module.exports = { DEFAULT_K, MIN_RUNS, ISSUES_MANAGER_TAG, ISSUES_MANAGER_LABEL, ISSUE_STATUS };
