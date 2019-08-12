---
layout: section
title: "Beam Release Guide: 05 - Triage JIRAs"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/triage-jira/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->


# Check the blockers


******

## Triage release-blocking issues in JIRA

There could be outstanding release-blocking issues, which should be triaged before proceeding to build a release candidate.
We track them by assigning a specific `Fix version` field even before the issue resolved.

The list of release-blocking issues is available at the 
[version status page](https://issues.apache.org/jira/browse/BEAM/?selectedTab=com.atlassian.jira.jira-projects-plugin:versions-panel). 
Triage each unresolved issue with one of the following resolutions:

For all JIRA issues:

* If the issue has been resolved and JIRA was not updated, resolve it accordingly.

For JIRA issues with type "Bug" or labeled "flaky":

* If the issue is a known continuously failing test, it is not acceptable to defer this until the next release. Please work with the Beam community to resolve the issue.
* If the issue is a known flaky test, make an attempt to delegate a fix. However, if the issue may take too long to fix (to the discretion of the release manager):
  * Delegate manual testing of the flaky issue to ensure no release blocking issues.
  * Update the `Fix Version` field to the version of the next release. Please consider discussing this with stakeholders and the dev@ mailing list, as appropriate.

For all other JIRA issues:

* If the issue has not been resolved and it is acceptable to defer this until the next release, update the `Fix Version` field to the new version you just created. Please consider discussing this with stakeholders and the dev@ mailing list, as appropriate.
* If the issue has not been resolved and it is not acceptable to release until it is fixed, the release cannot proceed. Instead, work with the Beam community to resolve the issue.

If there is a bug found in the RC creation process/tools, those issues should be considered high priority and fixed in 7 days.



*****


## Review Release Notes in JIRA

JIRA automatically generates Release Notes based on the `Fix Version` field applied to issues.
Release Notes are intended for Beam users (not Beam committers/contributors).
You should ensure that Release Notes are informative and useful.

Open the release notes from the 
[version status page](https://issues.apache.org/jira/browse/BEAM/?selectedTab=com.atlassian.jira.jira-projects-plugin:versions-panel) 
by choosing the release underway and clicking Release Notes.

You should verify that the issues listed automatically by JIRA are appropriate to appear in the Release Notes. 
Specifically, issues should:

* Be appropriately classified as `Bug`, `New Feature`, `Improvement`, etc.
* Represent noteworthy user-facing changes, such as new functionality, backward-incompatible API changes, or performance improvements.
* Have occurred since the previous release; an issue that was introduced and fixed between releases should not appear in the Release Notes.
* Have an issue title that makes sense when read on its own.

Adjust any of the above properties to the improve clarity and presentation of the Release Notes.



*****


## Review cherry-picks

Check if there are outstanding cherry-picks into the release branch, [e.g. for `2.14.0`](https://github.com/apache/beam/pulls?utf8=%E2%9C%93&q=is%3Apr+base%3Arelease-2.14.0).

Make sure they have blocker JIRAs attached and are OK to get into the release by checking with community if needed.


*****
<a class="button button--primary" href="{{'/contribute/release-guide/prepare-docs/'|prepend:site.baseurl}}">Next Step: Prepare Documentation</a>
