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
  constructor(pathToConfigFile) {
    this.config = yaml.load(
      fs.readFileSync(pathToConfigFile, { encoding: "utf-8" })
    );
  }

  // Given a list of labels and an exclusion list of reviewers not to include (e.g. the author)
  // returns all possible reviewers for each label
  getReviewersForLabels(
    labels: Label[],
    exclusionList: string[]
  ): { [key: string]: string[] } {
    let reviewersFound = false;
    let labelToReviewerMapping = {};
    labels.forEach((label) => {
      let reviewers = this.getReviewersForLabel(label.name, exclusionList);
      if (reviewers.length > 0) {
        labelToReviewerMapping[label.name] = reviewers;
        reviewersFound = true;
      }
    });
    if (!reviewersFound) {
      const fallbackReviewers = this.getFallbackReviewers(exclusionList);
      if (fallbackReviewers.length > 0) {
        labelToReviewerMapping[NO_MATCHING_LABEL] =
          this.getFallbackReviewers(exclusionList);
      }
    }
    return labelToReviewerMapping;
  }

  // Get possible reviewers excluding the author.
  getReviewersForLabel(label: string, exclusionList: string[]): string[] {
    var labelObjects = this.config.labels;
    const labelObject = labelObjects.find(
      (labelObject) => labelObject.name.toLowerCase() === label.toLowerCase()
    );
    if (!labelObject) {
      return [];
    }

    return this.excludeFromReviewers(labelObject.reviewers, exclusionList);
  }

  getExclusionListForLabel(label: string): string[] {
    var labelObjects = this.config.labels;
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
