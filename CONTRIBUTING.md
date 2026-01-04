<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Contributing to Apache Beam

Apache Beam welcomes many types of contributions, including code, documentation, and community help.

**Full guide:** https://beam.apache.org/contribute/

---

## Before Opening a Pull Request

Before creating a PR, please:

- **Search** for existing issues or PRs to avoid duplication
- **Create or comment** on an issue to share your intent
- **Discuss** large changes or first-time contributions on `dev@beam.apache.org`
- **Explain clearly** what you're changing and why

---

## Prerequisites

### Required Tools

- **GitHub account**
- **Operating System:** Linux, macOS, or Windows
- **Java JDK 11** (preferred; versions 8, 17, and 21 also supported)
- **Go 1.x** (latest version)
- **Docker** (for building worker containers and local testing)

### For SDK Development

**Python:**
- For manual testing: any supported Python version (see [gradle.properties](https://github.com/apache/beam/blob/master/gradle.properties))
- For test suites: all Python versions supported by Beam must be installed and available via `python3.x` commands
- Setup help: [Python Tips on Developer Wiki](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips)

### For Large Contributions

- Signed **Individual Contributor License Agreement (ICLA)** to Apache Software Foundation
- Download: https://www.apache.org/licenses/icla.pdf

---

## Share Your Intent

### 1. Find or Create an Issue

Visit: https://github.com/apache/beam/issues/new/choose

### 2. Use Issue Commands

Comment on the issue to manage it:
- **`.take-issue`** → assigns the issue to you
- **`.free-issue`** → unassigns the issue from you
- **`.close-issue`** → closes the issue when complete

> **Note:** If you're a committer assigning to a non-committer, they must comment first. The command is ignored if surrounded by markdown backticks.

### 3. Discuss Your Change

For large changes or first contributions:
- Email the mailing list: https://beam.apache.org/community/contact-us/

### 4. Create a Design Document (for large changes)

- Use the [design doc template](https://s.apache.org/beam-design-doc-template)
- See [design doc examples](https://s.apache.org/beam-design-docs)
- Email it to `dev@beam.apache.org`

---

## Environment Setup

### Helpful Resources

Check out the Wiki pages for tips on:
- [Git](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips)
- [Go](https://cwiki.apache.org/confluence/display/BEAM/Go+Tips)
- [Gradle](https://cwiki.apache.org/confluence/display/BEAM/Gradle+Tips)
- [Java](https://cwiki.apache.org/confluence/display/BEAM/Java+Tips)
- [Python](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips)

### Configuration Options

You have two main options:

#### Option 1: Local Setup

**Manual Installation (Debian-based systems):**
```bash
sudo apt-get install \
   openjdk-11-jdk \
   python-setuptools \
   python-pip \
   virtualenv \
   tox \
   docker-ce
```

For Ubuntu 20.04 and similar systems:
```bash
pip3 install grpcio-tools mypy-protobuf
```

**For Go Development:**
1. Install [Go](https://golang.org/doc/install)
2. Ensure Beam repo is in: `$GOPATH/src/github.com/apache/`
3. Final path should be: `$GOPATH/src/github.com/apache/beam`
4. Install goavro:
```bash
export GOPATH=`pwd`/sdks/go/examples/.gogradle/project_gopath
go get github.com/linkedin/goavro/v2
```

> **Important:** gLinux users should configure machines for sudoless Docker.

**Automated Script (Linux and macOS):**

Use the included setup script that installs:
- pip3 packages
- Go packages
- goavro
- JDK 11
- Python
- Docker
```bash
./local-env-setup.sh
```

#### Option 2: Docker-Based Setup

Use a Docker container that meets all requirements:
```bash
./start-build-env.sh
```

---

## Development Setup

### 1. Fork and Clone

See [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips) for help with forking, cloning, branching, and committing.

**Fork the repository:**
- Go to https://github.com/apache/beam and fork it

**Clone your fork:**
```bash
mkdir -p ~/path/to/your/folder
cd ~/path/to/your/folder
git clone https://github.com/<your-username>/beam
cd beam
```

**For Go Development:**

We recommend putting it in your `$GOPATH` (`$HOME/go` by default):
```bash
git clone https://github.com/apache/beam.git
cd beam
git remote add <GitHub_user> git@github.com:<GitHub_user>/beam.git
git fetch --all
```

Get or update Go SDK dependencies:
```bash
go get -u ./...
```

### 2. Verify Your Setup

**Option 1: Validate All Environments (Go, Java, Python)**

> **Important:** Activate Python development first.
```bash
./gradlew :checkSetup
```

**Option 2: Run Independent Checks**

**Go:**
```bash
export GOLANG_PROTOBUF_REGISTRATION_CONFLICT=ignore
./gradlew :sdks:go:examples:wordCount
```

**Python:**
```bash
./gradlew :sdks:python:wordCount
```

**Java:**
```bash
./gradlew :examples:java:wordCount
```

### 3. Follow the Code Change Guide

See [Code Change Guide](contributor-docs/code-change-guide.md) for:
- Making code changes
- Setting up unit tests
- Testing locally

### 4. Set Up Your IDE (Optional)

See developer Wiki guides for:
- [IntelliJ](https://cwiki.apache.org/confluence/display/BEAM/Using+IntelliJ+IDE)
- [Java Tips](https://cwiki.apache.org/confluence/display/BEAM/Java+Tips)
- [Python Tips](https://cwiki.apache.org/confluence/display/BEAM/Python+Tips)
- [Go Tips](https://cwiki.apache.org/confluence/display/BEAM/Go+Tips)
- [Website Tips](https://cwiki.apache.org/confluence/display/BEAM/Website+Tips)
- [Gradle Tips](https://cwiki.apache.org/confluence/display/BEAM/Gradle+Tips)
- [Jenkins Tips](https://cwiki.apache.org/confluence/display/BEAM/Jenkins+Tips)
- [Contributor FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ)

---

## Create a Pull Request

### 1. Make Your Code Change

- **Include Apache license headers** in every source file
- **Ensure open source compatibility** for any new dependencies ([Apache compatibility criteria](https://www.apache.org/legal/resolved.html#criteria))

### 2. Add Unit Tests

Write comprehensive unit tests for your changes.

### 3. Write Clear Commit Messages

Use descriptive commit messages that make it easy to identify changes and provide clear history.

### 4. Open Your Pull Request

When your change is ready for review:
- Create a pull request
- Link to the issue you're addressing

### 5. CI Tests

Pull requests trigger [pre-commit jobs](https://cwiki.apache.org/confluence/display/BEAM/Contribution+Testing+Guide#ContributionTestingGuide-Pre-commit) automatically.

**To re-run failed tests**, comment:
```
retest this please
```

**For post-commit tests**, see the [catalog of trigger phrases](https://github.com/apache/beam/blob/master/.test-infra/jenkins/README.md) in the PR template. Use these sparingly as they consume shared resources.

**For workflow issues**, see the [workflows README](https://github.com/apache/beam/blob/master/.github/workflows/README.md).

---

## Review Process

### Getting a Review

Reviewers should be assigned automatically within a few hours. If not:

**Find a reviewer:**
- Look for similar code merges, or
- Ask on `dev@beam.apache.org`
- Use `R: @username` in your PR to request a review

**If no response in 3 business days:**
- Email `dev@beam.apache.org` to request someone look at your PR

> **Note:** Only [Beam committers](https://home.apache.org/phonebook.html?pmc=beam) can merge pull requests.

### Making Reviews Easier

**Provide context:**
- Explain changes in the issue and PR description

**Avoid huge changes:**
- Break large changes into smaller PRs when possible

**Handle feedback effectively:**
- Add follow-up changes as **separate "fixup" commits**
- This allows reviewers to track progress and see incremental changes
- Keep comment threads attached to code
- **Don't squash reviewed commits** until review is complete
- Squashing makes it harder to see changes between iterations

**After approval:**
- Squash fixup commits (see [Git workflow tips](https://cwiki.apache.org/confluence/display/BEAM/Git+Tips))
- Committers can squash all commits during merge
- If PR has independent changes plus fixup commits, help squash the fixups

---

## Releases

### Release Schedule

- Apache Beam makes **minor releases every 6 weeks**
- View the [release calendar](https://calendar.google.com/calendar/embed?src=0p73sl034k80oob7seouanigd0%40group.calendar.google.com)
- Changes must be merged to master **before the release branch is cut**

### Stale Pull Requests

- PRs with no author response to comments for **60 days** will be closed
- Closed PRs can be reopened anytime

---

## Contributing Documentation

### Developer Documentation

New contributors often find gaps in documentation. To contribute:

**Option 1: Open a PR**
- Submit changes directly to the Beam repository

**Option 2: Edit the Wiki**
- Edit the [Beam Wiki](https://cwiki.apache.org/confluence/display/BEAM/Apache+Beam)
- Everyone has default access
- To contribute changes, create an account and request edit access on `dev@beam.apache.org` (include your Wiki user ID)

---

## Where to Contribute

### Starter Tasks

Great for getting started:
- https://s.apache.org/beam-starter-tasks

### Roadmap

For major efforts:
- https://beam.apache.org/roadmap/

---

## Additional Resources

### Guides and Documentation

- **Contributor FAQ:** https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ
- **PTransform Style Guide:** https://beam.apache.org/contribute/ptransform-style-guide
- **Runner Authoring Guide:** https://beam.apache.org/contribute/runner-guide/
- **Design Documents:** https://s.apache.org/beam-design-docs

---

## Troubleshooting

If you run into issues:

1. Check the [Contribution FAQ](https://cwiki.apache.org/confluence/display/BEAM/Contributor+FAQ)
2. Ask on the [dev@ mailing list](https://beam.apache.org/community/contact-us/)
3. Join the [#beam channel on ASF Slack](https://beam.apache.org/community/contact-us/)
4. [Reach out to the Beam community](https://beam.apache.org/community/contact-us/)

---

## Quick Reference

### Important Links

- **Main Guide:** https://beam.apache.org/contribute/
- **Repository:** https://github.com/apache/beam
- **Issues:** https://github.com/apache/beam/issues
- **Mailing List:** dev@beam.apache.org
- **Community Contact:** https://beam.apache.org/community/contact-us/
- **Wiki:** https://cwiki.apache.org/confluence/display/BEAM/Apache+Beam

### Commands

**Issue Management:**
```
.take-issue    # Assign to yourself
.free-issue    # Unassign from yourself
.close-issue   # Close the issue
```

**PR Testing:**
```
retest this please    # Re-run CI tests
R: @username         # Request review
```
