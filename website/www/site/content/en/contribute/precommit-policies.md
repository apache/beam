---
title: "Pre-commit Test Policies"
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

# Pre-commit test policies

## Definitions

- Pre-commit test - Any single test in a pre-commit test suite.
- Pre-commit test suite - A collection of pre-commit tests that have a common
  denominator. A test suite runs in a single Jenkins job. Currently, suites are
  grouped by SDK languages, e.g., Python, Java, and Go.

## Policies

### Pull Requests

- A PR must pass pre-commit tests before being committed to the main Beam repo.
  - The relevant pre-commit test suites are automatically launched according to
    PR contents.

### Problems

#### Breakage

Breakage is when one or more tests in a pre-commit test suite fails or
is flaky (occasionally fails).

- Breakages should be fixed within 8 hours.

#### Slowness

Slowness is when the total time to run a pre-commit suite exceeds 30 minutes\*,
including the time the job spends in the Jenkins queue.

- Slowness should be fixed within 24 hours.

\* See the [Pre-commit Slowness Triage
Guide](/contribute/precommit-triage-guide/) for a precise definition of slowness
and for information on dealing with slowness.

### Problem Resolution

For any problem, the options are, one of:

- Roll back the culprit PR.
- Roll out a fix within 24 hours.
- Disable the slow test or feature temporarily (make sure there's a tracking
  issue to re-enable it).

