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

export class Pr {
  public commentedAboutFailingChecks: boolean;
  public reviewersAssignedForLabels: { [key: string]: string };
  public nextAction: string;
  public stopReviewerNotifications: boolean;
  public remindAfterTestsPass: string[];
  public committerAssigned: boolean;

  constructor(propertyDictionary) {
    this.commentedAboutFailingChecks = false;
    this.reviewersAssignedForLabels = {}; // map of label to reviewer
    this.nextAction = "Author";
    this.stopReviewerNotifications = false;
    this.remindAfterTestsPass = []; // List of handles
    this.committerAssigned = false;

    if (!propertyDictionary) {
      return;
    }
    if (propertyDictionary) {
      if ("commentedAboutFailingChecks" in propertyDictionary) {
        this.commentedAboutFailingChecks =
          propertyDictionary["commentedAboutFailingChecks"];
      }
      if ("reviewersAssignedForLabels" in propertyDictionary) {
        this.reviewersAssignedForLabels =
          propertyDictionary["reviewersAssignedForLabels"];
      }
      if ("nextAction" in propertyDictionary) {
        this.nextAction = propertyDictionary["nextAction"];
      }
      if ("stopReviewerNotifications" in propertyDictionary) {
        this.stopReviewerNotifications =
          propertyDictionary["stopReviewerNotifications"];
      }
      if ("remindAfterTestsPass" in propertyDictionary) {
        this.remindAfterTestsPass = propertyDictionary["remindAfterTestsPass"];
      }
      if ("committerAssigned" in propertyDictionary) {
        this.committerAssigned = propertyDictionary["committerAssigned"];
      }
    }
  }

  // Returns a label that the reviewer is assigned for.
  // If none, returns an empty string
  getLabelForReviewer(reviewer: string): string {
    const labels = Object.keys(this.reviewersAssignedForLabels);
    for (let i = 0; i < labels.length; i++) {
      let label = labels[i];
      if (this.reviewersAssignedForLabels[label] === reviewer) {
        return label;
      }
    }

    return "";
  }

  // Returns whether any of the assigned reviewers are committers
  async isAnyAssignedReviewerCommitter(): Promise<boolean> {
    const labels = Object.keys(this.reviewersAssignedForLabels);
    for (let i = 0; i < labels.length; i++) {
      if (
        await github.checkIfCommitter(
          this.reviewersAssignedForLabels[labels[i]]
        )
      ) {
        return true;
      }
    }

    return false;
  }
}
