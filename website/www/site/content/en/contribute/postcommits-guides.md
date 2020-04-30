---
title: 'Post-commit tests processes guides'
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

# Post-commit test task guides

These guides provide steps for common post-commit test failure tasks.

## Finding someone to triage a post-commit test failure {#find_specialist}

To find the proper person to triage a test failure, you can use these
suggestions:

1.  If you can triage it yourself, go for it.
1.  Look at the GitHub blame for the files with problematic code.
1.  Ask in the [Beam Slack chat](https://the-asf.slack.com/messages/C9H0YNP3P/apps/A0F7VRFKN/).
1.  Write to the dev list: dev@beam.apache.org

## Rolling back a commit {#rollback}

Rolling back is usually the fastest way to fix a failing test.  However it is
is often inconvenient for the original author. To help the author fix the
issue, follow these steps when you rollback someone's change.

1.  Rollback the PR (or individual commit of the PR). The rollback PR should be green except in rare cases.
1.  Create a JIRA issue that contains the following information:
    * the reason for the rollback
    * a link to the test failure's JIRA issue
    * triage information
    * any other relevant details
1.  Assign the new JIRA issue to the original PR author.
1.  Consider re-opening the JIRA issue associated with the original PR (if
    there is one).
1.  Send a notification email with information about the rollback, links to the
    original PR and the rollback PR, and the reasons for the rollback to:
    *   dev@beam.apache.org
    *   the original PR author and the committer of the PR
1.  Close the test failure JIRA issue. Your work is done here!

## Disabling a failing test {#disabling}

If a test fails, our first priority is to rollback the problematic code and fix
the issue. However, if both: rollback and fix will take awhile to implement, it
is safer to temporarily disable the test until the fix is ready.

Use caution when deciding to disable a test. When tests are disabled,
contributors are no longer developing on top of fully tested code. If you decide
to disable a test, use the following guidelines:

*   Notify the dev@beam.apache.org mailing list. Describe the problem and let
    everyone know which test you are disabling.
*   Implement the fix and get the test back online as soon as possible.

While the test is disabled, contributors should not push code to the failing
test's coverage area. The code area is not properly tested until you fix the
test.


