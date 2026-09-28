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

const github = require("./githubUtils");

export class ReviewersForLabel {
  public label: string;
  public dateOfLastReviewAssignment: { [key: string]: number };

  constructor(
    label: string,
    propertyDictionary: {
      dateOfLastReviewAssignment: { [key: string]: number };
    }
  ) {
    this.label = label;
    this.dateOfLastReviewAssignment = {}; // map of reviewer to date

    if (!propertyDictionary) {
      return;
    }
    if ("dateOfLastReviewAssignment" in propertyDictionary) {
      this.dateOfLastReviewAssignment =
        propertyDictionary["dateOfLastReviewAssignment"];
    }
  }

  private findLeastRecentlyAssignedReviewer(
    availableReviewers: string[]
  ): string {
    if (!availableReviewers || availableReviewers.length === 0) {
      return "";
    }

    if (!this.dateOfLastReviewAssignment[availableReviewers[0]]) {
      return availableReviewers[0];
    }

    let earliestDate = this.dateOfLastReviewAssignment[availableReviewers[0]];
    let earliestReviewer = availableReviewers[0];

    for (let i = 0; i < availableReviewers.length; i++) {
      let availableReviewer = availableReviewers[i];
      if (!this.dateOfLastReviewAssignment[availableReviewer]) {
        return availableReviewer;
      }
      if (earliestDate > this.dateOfLastReviewAssignment[availableReviewer]) {
        earliestDate = this.dateOfLastReviewAssignment[availableReviewer];
        earliestReviewer = availableReviewer;
      }
    }

    return earliestReviewer;
  }

  // Given a list of available reviewers,
  // returns the next reviewer up based on who has reviewed least recently.
  // Updates this object to reflect their assignment.
  assignNextReviewer(availableReviewers: string[]): string {
    if (availableReviewers.length === 0) {
      throw new Error(`No reviewers available for label ${this.label}`);
    }

    const chosenReviewer =
      this.findLeastRecentlyAssignedReviewer(availableReviewers);
    this.dateOfLastReviewAssignment[chosenReviewer] = Date.now();
    return chosenReviewer;
  }

  // Returns a backup reviewer from the same label (excluding the main reviewer)
  // without updating dateOfLastReviewAssignment. Falls back to fallbackReviewers
  // if the label has no other eligible reviewers.
  getBackupReviewer(
    availableReviewers: string[],
    mainReviewer: string,
    fallbackReviewers: string[] = []
  ): string {
    const remainingLabelReviewers = (availableReviewers || []).filter(
      (reviewer) => reviewer !== mainReviewer
    );
    if (remainingLabelReviewers.length > 0) {
      return this.findLeastRecentlyAssignedReviewer(remainingLabelReviewers);
    }

    const remainingFallbackReviewers = (fallbackReviewers || []).filter(
      (reviewer) => reviewer !== mainReviewer
    );
    return this.findLeastRecentlyAssignedReviewer(remainingFallbackReviewers);
  }

  // Given the up to date list of available reviewers (excluding the author),
  // returns the next reviewer up based on who has reviewed least recently.
  // Updates this object to reflect their assignment.
  async assignNextCommitter(
    availableReviewers: string[],
    fallbackReviewers: string[]
  ): Promise<string> {
    let earliestDate = Date.now();
    let earliestCommitter: string = "";

    for (let i = 0; i < availableReviewers.length; i++) {
      let availableReviewer = availableReviewers[i];
      if (await github.checkIfCommitter(availableReviewer)) {
        if (!this.dateOfLastReviewAssignment[availableReviewer]) {
          this.dateOfLastReviewAssignment[availableReviewer] = Date.now();
          return availableReviewer;
        }
        if (earliestDate > this.dateOfLastReviewAssignment[availableReviewer]) {
          earliestDate = this.dateOfLastReviewAssignment[availableReviewer];
          earliestCommitter = availableReviewer;
        }
      }
    }

    if (!earliestCommitter) {
      console.log(`No committers available for label ${this.label}`);
      console.log(`Using fallbackReviewers label instead of ${this.label}`);
      return this.assignNextCommitter(fallbackReviewers, fallbackReviewers);
    }
    this.dateOfLastReviewAssignment[earliestCommitter] = Date.now();
    return earliestCommitter;
  }
}
