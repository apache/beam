---
layout: section
title: 'Post-commit tests policies'
section_menu: section-menu/contribute.html
permalink: /contribute/postcommits-policies/
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

# Post-commit tests policies

Post-commit tests validate that Beam works correctly in a live environment. The
tests also catch errors that are hard to predict in the design and
implementation stages

Even though post-commit tests run after the code is merged into the repository,
it is important that the tests pass reliably. Jenkins executes post-commit tests
against the HEAD of the master branch. If post-commit tests fail, there is a
problem with the HEAD build. In addition, post-commit tests are time consuming
to run, and it is often hard to triage test failures.


## Policies {#policies}

To ensure that Beam's post-commit tests are reliable and healthy, the Beam
community follows these post-commit test policies:

*   [Rollback first]({{ site.baseurl }}/contribute/postcommits-policies-details/index.html#rollback_first)
*   [A failing test is a critical bug]({{ site.baseurl }}/contribute/postcommits-policies-details/index.html#failing_test_is_critical_bug)
*   [A flaky test is a critical bug]({{ site.baseurl }}/contribute/postcommits-policies-details/index.html#flake_is_failing)
*   [Flaky tests must either be fixed or removed]({{ site.baseurl }}/contribute/postcommits-policies-details/index.html#remove_flake)
*   [Fixes for post-commit failures should include a corresponding new pre-commit test]({{ site.baseurl }}/contribute/postcommits-policies-details/index.html#precommit_for_postcommit)


## Post-commit test failure scenarios

When a post-commit test fails, follow the provided steps for your situation.

### I found a test failure {#found-failing-test}

1.  Create a [JIRA issue](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20test-failures)
    and assign it to yourself.
1.  Do high level triage of the failure.
1.  [Assign the JIRA issue to a relevant person]({{ site.baseurl }}/contribute/postcommits-guides/index.html#find_specialist).

### I was assigned a JIRA issue for a test failure {#assigned-failing-test}

1.  [Rollback the culprit change]({{ site.baseurl }}/contribute/postcommits-guides/index.html#rollback).
1.  If you determine that rollback will take longer than 8 hours, [disable the
    test temporarily]({{ site.baseurl }}/contribute/postcommits-guides/index.html#disabling) while you rollback or create a
    fix.

> Note: Rollback is always the first course of action. If a fix is trivial,
> open a pull request with the proposed fix while doing rollback.

### My change was rolled back due to a test failure {#pr-rolled-back}

1.  Look at the JIRA issue to find the reason for the rollback.
1.  Fix your code and re-run the post-commit tests.
1.  Implement new pre-commit tests that will catch similar bugs before future
    code is merged into the repository.
1.  Open a new PR that contains your fix and the new pre-commit tests.

## Useful links

*   [Best practices for writing tests]({{ site.baseurl }}/contribute/testing/index.html#best_practices)

## References

1.  [Keeping post-commit tests green](https://lists.apache.org/thread.html/3bb4aa777751da2e2d7e22666aa6a2e18ae31891cb09d91718b75e74@%3Cdev.beam.apache.org%3E)
    mailing list proposal thread.
