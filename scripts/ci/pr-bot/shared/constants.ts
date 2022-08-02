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

const path = require("path");

export const REPO_OWNER = "apache";
export const REPO = "beam";
export const PATH_TO_CONFIG_FILE = path.join(
  __dirname,
  "../../../../../.github/REVIEWERS.yml"
);
export const PATH_TO_METRICS_CSV = path.resolve(
  path.join(__dirname, "../../metrics.csv")
);
export const BOT_NAME = "github-actions";
export const REVIEWERS_ACTION = "Reviewers";
export const SLOW_REVIEW_LABEL = "slow-review";
export const NO_MATCHING_LABEL = "no-matching-label";
