---
layout: section
title: "Precommit Slowness Triage Guide"
permalink: /contribute/precommit-triage-guide/
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

# Precommit Slowness Triage Guide

Beam precommit jobs are suites of tests run automatically on Jenkins build
machines for each pull request (PR) submitted to
[apache/beam](https://github.com/apache/beam). For more information and the
difference between precommits and postcommits, see
[testing](/contribute/testing/).

## What are fast precommits?

Precommit tests are required to pass before a pull request (PR) may be merged.
When these tests are slow they slow down Beam's development process.

The aim is to have 95% of precommit jobs complete within 30 minutes
(failing or passing).
Technically, the 95th percentile of running time should be below 30 minutes over
the past 4 weeks, where running time is the duration of time the job spends in
the Jenkins queue + the actual time it spends running.

## Detemining Slowness

The current method for determining if precommmits are slow is to look at the
[Jupyter
notebook](https://github.com/apache/beam/tree/master/.test-infra/jupyter)
`precommit_job_times.ipynb`.

Run the notebook. It should output a table with running times. The numbers in
the column `totalDurationMinutes_all` and the rows with a `job_name` like `4
weeks 95th` contain the target numbers for determining slowness.
If any of these numbers are above 30, triaging is required.

### Example
Here's an example table of running times:
![example precommit duration table](/images/precommit_durations.png)

In this example, Go precommits are taking approximately 14 minutes, which is
fast. Java and Python precommits are taking 78 and 32 minutes respectively,
which is slow. Both Java and Python precommits require triage.

## Triage Process

1. [Search for existing
   issues](https://issues.apache.org/jira/issues/?filter=12344461)
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

It is expected that slowness is resolved promptly. See [precommit test
policies](/contribute/precommit-policies/) for details.

## Possible Causes and Solutions

This section lists some starting off points for fixing precommit slowness.

### Jenkins

Have a look at the graphs in the Jupyter notebook. Does the rise in total
duration match the rise in queuing time? If so, the slowness might be unrelated
to this specific precommit job.

Example of when total and queuing durations rise and fall together (mostly):
![graph of precommit times](/images/precommit_graph_queuing_time.png)

Since Jenkins machines are a limited resource, other jobs can
affect precommit queueing times. Try to figure out if other jobs have been
recently slower, increased in frequency, or new jobs have been introduced.

Another option is to look at adding more Jenkins machines.

### Slow individual tests

Sometimes a precommit job is slowed down due to one or more tests. One way of
determining if this is the case is by looking at individual test timings.

Where to find individual test timings:

- Look at the `Gradle Build Scan` link on the precommit job's Jenkins page. This
  page will contain individual test timings for Java tests only (2018-08).
- Look at the `Test Result` link on the precommit job's Jenkins page. This
  should be available for Java and Python tests (2018-08).

Sometimes tests can be made faster by refactoring. A test that spends a lot of
time waiting (such as an integration test) could be made to run concurrently with
the other tests.

If a test is determined to be too slow to be part of precommit tests, it could
be removed from precommit and placed in postcommit instead (but it should be in
postcommit already). In addition, ensure that the code covered by the removed
test is covered by a unit test in precommit.

### Slow integration tests

Integration test slowdowns may be caused by dependent services.

## References

- [Beam Fast Precommits design doc](https://docs.google.com/document/d/1udtvggmS2LTMmdwjEtZCcUQy6aQAiYTI3OrTP8CLfJM/edit?usp=sharing)
