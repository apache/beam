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

const yaml = require("js-yaml");
const fs = require("fs");
import { Label } from "./githubUtils";
const { NO_MATCHING_LABEL } = require("./constants");

export class ReviewerConfig {
  private config: any;
  private configPath: string;
  constructor(pathToConfigFile) {
    this.config = yaml.load(
      fs.readFileSync(pathToConfigFile, { encoding: "utf-8" })
    );
    this.configPath = pathToConfigFile;
  }

  // Returns all possible reviewers for each label configured.
  getReviewersForAllLabels(): { [key: string]: string[] } {
    const labelObjects = this.config.labels;
    let reviewersForLabels = {};
    for (const labelObject of labelObjects) {
      reviewersForLabels[labelObject.name.toLowerCase()] =
        labelObject.reviewers;
    }

    return reviewersForLabels;
  }

  // Given a list of labels and an exclusion list of reviewers not to include (e.g. the author)
  // returns all possible reviewers for the single highest-priority matching label
  // (lower priority number = higher priority).
  getReviewersForLabels(
    labels: Label[],
    exclusionList: string[]
  ): { [key: string]: string[] } {
    let labelToReviewerMapping: { [key: string]: string[] } = {};
    let bestLabel: string | null = null;
    let bestPriority: number = Number.MAX_SAFE_INTEGER;
    let bestReviewers: string[] = [];

    for (const label of labels || []) {
      let reviewers = this.getReviewersForLabel(label.name, exclusionList);
      if (reviewers.length > 0) {
        const priority = this.getPriorityForLabel(label.name);
        if (bestLabel === null || priority < bestPriority) {
          bestLabel = label.name;
          bestPriority = priority;
          bestReviewers = reviewers;
        }
      }
    }
    if (bestLabel !== null) {
      labelToReviewerMapping[bestLabel] = bestReviewers;
    } else {
      const fallbackReviewers = this.getFallbackReviewers(exclusionList);
      if (fallbackReviewers.length > 0) {
        labelToReviewerMapping[NO_MATCHING_LABEL] = fallbackReviewers;
      }
    }
    return labelToReviewerMapping;
  }

  getPriorityForLabel(label: string): number {
    const labelObjects = this.config.labels;
    const labelObject = labelObjects.find(
      (labelObject) => labelObject.name.toLowerCase() === label.toLowerCase()
    );
    if (!labelObject || typeof labelObject.priority !== "number") {
      return Number.MAX_SAFE_INTEGER;
    }
    return labelObject.priority;
  }

  // Get possible reviewers excluding the author.
  getReviewersForLabel(label: string, exclusionList: string[]): string[] {
    const labelObjects = this.config.labels;
    const labelObject = labelObjects.find(
      (labelObject) => labelObject.name.toLowerCase() === label.toLowerCase()
    );
    if (!labelObject) {
      return [];
    }

    return this.excludeFromReviewers(labelObject.reviewers, exclusionList);
  }

  updateReviewerForLabel(label: string, reviewers: string[]) {
    const labelIndex = this.config.labels.findIndex(
      (labelObject) => labelObject.name.toLowerCase() === label.toLowerCase()
    );
    this.config.labels[labelIndex].reviewers = reviewers;

    const contents = `# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# If you would like to be excluded from consideration for reviewing a certain label,
# add yourself to that label's exclusionList
# FallbackReviewers is for reviewers who can review any area of the code base that might
# not receive a label. These should generally be more experienced committers.
# Priority determines which label is used to select a reviewer when a PR has multiple labels
# (lower number = higher priority).
${yaml.dump(this.config)}`;

    fs.writeFileSync(this.configPath, contents, { encoding: "utf-8" });
  }

  getExclusionListForLabel(label: string): string[] {
    const labelObjects = this.config.labels;
    const labelObject = labelObjects.find(
      (labelObject) => labelObject.name.toLowerCase() === label.toLowerCase()
    );
    return labelObject?.exclusionList ?? [];
  }

  // Get fallback reviewers excluding the author.
  getFallbackReviewers(exclusionList: string[]): string[] {
    return this.excludeFromReviewers(
      this.config.fallbackReviewers,
      exclusionList
    );
  }

  private excludeFromReviewers(
    reviewers: string[],
    exclusionList: string[]
  ): string[] {
    if (!exclusionList) {
      return reviewers;
    }

    return reviewers.filter(
      (reviewer) => exclusionList.indexOf(reviewer) == -1
    );
  }
}
