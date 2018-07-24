---
layout: section
title: 'Post-commit tests processes guides'
permalink: /contribute/postcommits-guides/
section_menu: section-menu/contribute.html
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

# Post-commit tests processes guides

[TOC]


## Finding person to fix post-commit tests failures {#find_specialist}

Finding proper person to triage the test failure might not be a trivial task.
However there are some guidelines.

1.  If you can do it -- go for it.
1.  Check GitHub blame on files with problematic code.
1.  Reach out to
    [Slack chat](https://the-asf.slack.com/messages/C9H0YNP3P/apps/A0F7VRFKN/).
1.  Reach out to dev@beam.apache.org.


## Rolling back commit {#rollback}

Most likely, you are on this page because there is a failing post-commit test
and you want to rollback culprit commit.

Since rolling back someones change might be inconvenient for the person that
made the original change, we ask you to do some extra steps. These steps are
intended to help that person fix the issue that caused test failure and get his
feature back in line.

1.  Rollback the PR
1.  Create JIRA task with information on why the code rolled back, links to
    corresponding Tests failure task, triage information, any other relevant
    information. Assign this task to original PR author.
1.  Consider re-opening Jira ticket that was closed by original PR if any.
1.  Send notification email with information of roll-back, links to original PR,
    roll-back PR, reasons for roll-back to:
    *   dev@beam.apache.org
    *   Original PR author and committer of that PR.
1.  Close test-failure Jira task. Your work is done here.


## Disabling failing test {#disabling}

Usually we want our tests green. If they eventually turn red, we fix them or do
rollback.

However sometimes it might take us too much time to fix some specific test. In
this situation it might be feasible to disable a single test, so that the rest
of the suite give valid health signal.

However this should be done with great caution, since disabling a test means
that we build upon shaky, not fully tested foundation.

Because of this we want to do some extra precautions if we decide to follow this
path:

*   Notify everyone on dev list that some test is being disabled and describe
    the problem.
*   It is everyones job at this point to avoid pushing more code to the area
    that failing test covered, since this code will not be properly tested at
    this point.
*   Do the fix and get the test back in line as soon as possible.
