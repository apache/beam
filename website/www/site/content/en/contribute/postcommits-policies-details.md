---
title: 'Post-commit policies details'
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

# Post-commit policies details

A post-commit test failure means that there is a bug in the code. The longer the
bug exists, the harder it is to fix it due to ongoing code contributions. As a
result, we want to fix bugs quickly. The Beam community's post-commit test
policies help keep our code and test results in a good state.


## Rollback first {#rollback_first}

Beam uses a "rollback first" approach: the first action to resolve a test
failure is to rollback the culprit code change. The two main benefits of this
approach are short implementation time and high reliability. When we rollback
first, we quickly return to a previously verified good state.

At a high level, this approach consists of the following steps:

1.  Revert the culprit commit.
1.  Re-run the post-commit tests to verify the tests pass.
1.  Push the revert commit.

For background on this policy, see the
[mailing list thread](https://lists.apache.org/thread.html/3bb4aa777751da2e2d7e22666aa6a2e18ae31891cb09d91718b75e74@%3Cdev.beam.apache.org%3E)
and [design doc](https://docs.google.com/document/d/1sczGwnCvdHiboVajGVdnZL0rfnr7ViXXAebBAf_uQME/edit).


## A failing test is a critical/P1 issue {#failing_test_is_critical_bug}

It is difficult to properly verify new changes made on top of buggy code. In
some cases, adding additional code can make the problem worse. To avoid this
situation, fixing failing tests is our highest priority.


## A flaky test is a critical/P1 issue {#flake_is_failing}

Flaky tests are considered failing tests, and fixing a flaky test is a
critical/P1 issue.

Flaky tests are tests that randomly succeed or fail while using the same code
version. Flaky test failures are one of the most dangerous types of failures
because they are easy to ignore -- another run of the flaky test might pass
successfully. However, these failures can hide real bugs and flaky tests often
slowly accumulate. Someone must repeatedly triage the failures, and flaky tests
are often the hardest ones to fix.

Flaky tests do not provide a reliable quality signal, so it is important to
quickly fix the flakiness. If a fix will take awhile to implement, it is safer
to disable the test until the fix is ready.

Martin Fowler has a good [article](https://martinfowler.com/articles/nonDeterminism.html)
about non-determinism in tests.


## Flaky tests must be fixed or removed {#remove_flake}

Flaky tests do not provide a reliable quality signal, which has a harmful effect
on all tests and can lead to a loss of trust in our test suite. As a result,
contributors might start to ignore test failures.

We want everyone to trust our tests, so it is important to diligently fix all
flaky tests. If it is not possible to fix a flaky test, we must remove the test.


## Add new pre-commit tests as part of a post-commit fix {#precommit_for_postcommit}

Post-commit tests are an important fail-safe, but we want to fail fast. Failing
fast means that we want to detect bugs in pre-commit tests, and _not_ in
post-commit tests.

When you implement a fix for a post-commit test failure, add a new pre-commit
test that will detect similar failures in the future. For example, you can
implement a new unit test that covers a problematic code branch.

## Inform the community if Beam breaks downstream projects {#inform_community}

There are multiple external projects depending on Beam which contain tests that are
outside of Beam repository. For example, Dataflow, Samza runner, and IBM Streams.

When an external project encounters an issue caused by (a PR) in Beam
and, in consequence, requests for a change in the Beam repository,
the first thing is to create a JIRA entry that addresses
the following three questions:

1. Descriptions on what the issue is.
2. Does a revert fix it? (Or it is supposed to be fixed differently)
3. Is a revert the best way to fix it?

It is encouraged to bring the discussion to the dev mailing list as well.
Ideally, after the incident, we prefer to have discussions regarding
whether we should extend tests in Beam repository, with the goal of
catching similar issues early in the future.
