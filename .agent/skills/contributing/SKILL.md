---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name: contributing
description: Guides the contribution workflow for Apache Beam, including creating PRs, issue management, code review process, and release cycles. Use when contributing code, creating PRs, or understanding the contribution process.
---

# Contributing to Apache Beam

## Getting Started

### Prerequisites
- GitHub account
- Java JDK 11 (preferred, or 8, 17, 21)
- Latest Go 1.x
- Docker
- Python (any supported version for manual testing, all versions for running test suites)
- For large contributions: signed ICLA to Apache Software Foundation

### Environment Setup Options

#### Local Setup (automated)
```bash
./local-env-setup.sh
```

#### Docker-based Setup
```bash
./start-build-env.sh
```

## Contribution Workflow

### 1. Find or Create an Issue
- Search existing issues at https://github.com/apache/beam/issues
- Create new issue using appropriate template

### 2. Claim the Issue
```
.take-issue    # Assigns issue to you
.free-issue    # Unassigns issue from you
.close-issue   # Closes the issue
```

### 3. For Large Changes
- Discuss on dev@beam.apache.org mailing list
- Create design doc using [template](https://s.apache.org/beam-design-doc-template)
- Review [existing design docs](https://s.apache.org/beam-design-docs)

### 4. Make Your Changes
- Every source file needs Apache license header
- New dependencies must have Apache-compatible open source licenses
- Add unit tests for your changes
- Use descriptive commit messages

### 5. Create Pull Request
- Link to the issue in PR description
- Pre-commit tests run automatically
- If tests fail unrelated to your change, comment: `retest this please`

### 6. Code Review
- Reviewers are auto-assigned within a few hours
- Use `R: @username` to request specific reviewer
- No response in 3 days? Email dev@beam.apache.org

## Code Review Best Practices

### For Authors
- Provide context in issue and PR description
- Avoid huge mega-changes
- Add follow-up changes as "fixup" commits (don't squash until approved)
- Squash fixup commits after approval

### For Reviewers
- PRs can only be merged by [Beam committers](https://home.apache.org/phonebook.html?pmc=beam)

## Testing Workflows

### Pre-commit Tests
Run automatically on PRs. To run locally:
```bash
./gradlew javaPreCommit      # Java
./gradlew :sdks:python:test  # Python
./gradlew :sdks:go:test      # Go
```

### Post-commit Tests
Run after merge. Trigger phrases in PR comments start specific test suites.
See [trigger phrase catalog](https://github.com/apache/beam/blob/master/.test-infra/jenkins/README.md).

## Formatting

### Java
```bash
./gradlew spotlessApply
```

### Python
```bash
# Uses yapf, isort, pylint
pre-commit run --all-files
```

### CHANGES.md
```bash
./gradlew formatChanges
```

## Release Cycle
- Minor releases every 6 weeks
- Check [release calendar](https://calendar.google.com/calendar/embed?src=0p73sl034k80oob7seouanigd0%40group.calendar.google.com)
- Changes must be in master before release branch is cut

## Stale PRs
- PRs become stale after 60 days of author inactivity
- Community will close stale PRs
- Authors can reopen closed PRs

## Key Resources
- [Contribution Guide](https://beam.apache.org/contribute/)
- [PTransform Style Guide](https://beam.apache.org/contribute/ptransform-style-guide)
- [Runner Authoring Guide](https://beam.apache.org/contribute/runner-guide/)
- [Wiki Tips](https://cwiki.apache.org/confluence/display/BEAM/)
  - [Git Tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips)
  - [Java Tips](https://cwiki.apache.org/confluence/display/BEAM/Java+Tips)
  - [Python Tips](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips)
  - [Go Tips](https://cwiki.apache.org/confluence/display/BEAM/Go+Tips)
  - [Gradle Tips](https://cwiki.apache.org/confluence/display/BEAM/Gradle+Tips)

## Communication
- User mailing list: user@beam.apache.org
- Dev mailing list: dev@beam.apache.org
- Slack: [#beam channel](https://s.apache.org/beam-slack-channel)
- Issues: https://github.com/apache/beam/issues
