---
title: "Beam Feature Branches"
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

# Feature Branches

Some larger features are developed on a feature branch before being merged into
`master`. In particular, this is often used for initial development of new
components like SDKs or runners.

We expect the work on a feature branch to be _incomplete_, but it must not
be lower quality. Code reviews for feature branches must have the same
standards as code reviews for `master`. Once a feature branch is ready for
merge to `master`, the set of changes will be too large to review in its
entirety. Because of this, the code reviews during development must be
thorough and trustworthy.

## Establishing a feature branch

If your project is large enough to need a feature branch, there should
be a discussion on the mailing list. The first step is to [engage](/contribute/#connect-with-the-beam-community) there to raise awareness
that you want to start a large project. Almost any project should be accepted
-- there is no real cost to letting a feature branch exist -- but you may find
other interested contributors or gain other advice from the community.

After the community discussion, a committer must create your feature branch.
Any committer can do create the branch through the GitHub UIs or by pushing
directly to GitHub or ASF's gitbox.

## Developing on a feature branch

To contribute code on a feature branch, use the same process as in the
[Contribution Guide](/contribute/contribution-guide/), but
replace `master` with the name of the feature branch.

Since feature branches are often used for new components, you may find that
there is no committer familiar with all the details of the new language or
runner. In that case, consider asking someone else familiar with the technology
to do an initial review before looping in a committer for a final review and
merge.

If you are working on a feature branch, you'll also want to frequently merge in
changes from `master`. This prevents the feature branch from
deviating too far from the current state of `master`. Like all changes, this
should be done via pull request. A committer may self-merge such a pull request
if there are no conflicts or test failures. If there are any conflicts or tests
that need fixing, then those should get a full review from another committer.

## Merging into `master`

To merge a feature branch into `master`, new components and major features
should meet the following guidelines.

1. Have at least 2 contributors interested in maintaining it, and 1 committer
   interested in supporting it
2. Provide both end-user and developer-facing documentation
3. Have at least a basic level of unit test coverage
4. Run all existing applicable integration tests with other Beam components and
   create additional tests as appropriate

### Merging a new runner into `master`

A new runner should:

1. Be able to handle a subset of the model that addresses a significant set of
   use cases, such as ‘traditional batch’ or ‘processing time streaming’.
2. Update the capability matrix with the current status
3. Add a webpage under `documentation/runners`

### Merging a new SDK into `master`

A new SDK should:

1. Provide the ability to construct graphs with all the basic building blocks
   of the model (ParDo, GroupByKey, Window, Trigger, etc)
2. Begin fleshing out the common composite transforms (Count, Join, etc) and I/O
   connectors (Text, Kafka, etc)
3. Have at least one runner that can execute the complete model (may be a
   direct runner)
4. Provide integration tests for executing against current and future runners
5. Add a webpage under `documentation/sdks`

