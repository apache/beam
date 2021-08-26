---
title: "Pre-commit Slowness Triage Guide"
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

# Pre-commit Slowness Triage Guide

Beam pre-commit jobs are suites of tests run automatically on Jenkins build
machines for each pull request (PR) submitted to
[apache/beam](https://github.com/apache/beam). For more information and the
difference between pre-commits and post-commits, see
[testing](https://cwiki.apache.org/confluence/display/BEAM/Contribution+Testing+Guide).

## What are fast pre-commits?

Pre-commit tests are required to pass before a pull request (PR) is merged.
When these tests are slow they slow down Beam's development process.
The aim is to have 95% of pre-commit jobs complete within 30 minutes
(failing or passing).

Technically, the 95th percentile of running time should be below 30 minutes over
the past 4 weeks, where running time is the duration of time the job spends in
the Jenkins queue + the actual time it spends running.

## Determining Slowness

There are two main signs of slowness:

1. Pre-commit jobs are timing out after 30 minutes. This can be determined from
   the console log of a job.
1. Pre-commits aren't timing out, but the total wait time for pre-commit results
   is >30m.

### Pre-commit Dashboard

The Beam Community Metrics site contains a [Pre-Commit
Tests](http://metrics.beam.apache.org/d/_TNndF2iz/pre-commit-tests) dashboard showing
job timing trends. You can modify the time window (defaults to 7 days) or filter
down to a specific test suite by clicking on it.

![example pre-commit duration dashboard](/images/precommit_dashboard.png)

## Triage Process

1. [Search for existing
   issues](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20status%20in%20(Open%2C%20%22In%20Progress%22%2C%20Reopened)%20AND%20labels%20%3D%20precommit%20ORDER%20BY%20priority%20DESC%2C%20updated%20DESC)
1. Create a new issue if needed: [Apache
   JIRA](https://issues.apache.org/jira/issues)
  - Project: Beam
  - Components: testing, anything else relevant
  - Label: precommit
  - Reference this page in the description.
1. Determine where the slowness is coming from and identify issues. Open
   additional issues if needed (such as for multiple issues).
1. Assign the issue as appropriate, e.g., to the test's or PR's author.

## Resolution

It is important that we quickly fix slow pre-commit tests. See [pre-commit test
policies](/contribute/precommit-policies/) for details.

## Possible Causes and Solutions

This section lists some starting points for fixing pre-commit slowness.

### Resource Exhaustion

Have a look at the graphs in the Jupyter notebook. Does the rise in total
duration match the rise in queuing time? If so, the slowness might be unrelated
to this specific pre-commit job.

Example of when total and queuing durations rise and fall together (mostly):
![graph of pre-commit times](/images/precommit_graph_queuing_time.png)

Since Jenkins machines are a limited resource, other jobs can
affect pre-commit queueing times. Try to figure out if other jobs have been
recently slower, increased in frequency, or new jobs have been introduced.

Another option is to look at adding more Jenkins machines.

### Slow individual tests

Sometimes a pre-commit job is slowed down due to one or more tests. One way of
determining if this is the case is by looking at individual test timings.

Where to find individual test timings:

- Look at the `Gradle Build Scan` link on the pre-commit job's Jenkins page.
  This page will contain individual test timings for Java tests only (2018-08).
- Look at the `Test Result` link on the pre-commit job's Jenkins page. This
  should be available for Java and Python tests (2018-08).

Sometimes tests can be made faster by refactoring. A test that spends a lot of
time waiting (such as an integration test) could be made to run concurrently with
the other tests.

If a test is determined to be too slow to be part of pre-commit tests, it should
be removed from pre-commit and placed in post-commit instead. In addition,
ensure that the code covered by the removed test is [covered by a unit test in
pre-commit](/contribute/postcommits-policies-details/#precommit_for_postcommit).

### Slow integration tests

Integration test slowdowns may be caused by dependent services.

## References

- [Beam Fast Precommits design doc](https://docs.google.com/document/d/1udtvggmS2LTMmdwjEtZCcUQy6aQAiYTI3OrTP8CLfJM/edit?usp=sharing)
