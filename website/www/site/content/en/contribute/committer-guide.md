---
title: "Beam Committer Guide"
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

# Committer Guide

This guide is for
[committers](https://www.apache.org/foundation/how-it-works.html#committers)
and covers Beam's guidelines for reviewing and merging code.

## Pull request review objectives

The review process aims for:

* Review iterations should be efficient, timely and of quality (avoid tiny or out-of-context changes or huge mega-changes)
* Support efficiency of authoring (don't want to wait on a review for a tiny bit because GitHub makes it very hard to stack up reviews in sequence / don't want to have major changes blocked because of difficulty of review)
* Ease of first-time contribution (encourage to follow [contribution guildelines](/contribute/#contributing-code)
  but committer may absorb some extra effort for new contributors)
* Pull requests and commit messages establish a clear history with purpose and origin of changes
* Ability to perform a granular rollback, if necessary (also see [policies](/contribute/postcommits-policies/))

Granularity of changes:

* We prefer small independent, incremental PRs with descriptive, isolated commits. Each commit is a single clear change
* It is OK to keep separate commits for different logical pieces of the code, if they make reviewing and revisiting code easier
* Making commits isolated is a good practice, authors should be able to relatively easily split the PR upon reviewer's request
* Generally, every commit should compile and pass tests.
* Avoid keeping in history formatting messages such as checkstyle or spotless fixes.  Squash such commits with previous one.

## Always get to LGTM ("Looks good to me!")

After a pull request goes through rounds of reviews and revisions, it will
become ready for merge. A reviewer signals their approval either
by GitHub "approval" or by a comment such as "Looks good to me!" (LGTM).

 - If the author of the pull request is not a committer, a committer must be
   the one to approve the change.
 - If the author of the pull request is a committer, approval from their chosen
   reviewer is enough. A committer is trusted to choose an appropriate
   reviewer, even if the reviewer is not a committer.

Once a pull request is approved, any committer can merge it.

Exceptions to this rule are rare and made on a case-by-case basis. A committer
may use their discretion for situations such as build breaks. In this case, you
should still seek a review on the pull request!  A common acronym you may see
is "TBR" -- "to be reviewed".

**Always go through a pull request, even if you won’t wait for the code
review.** Committers should never commit anything without going through a pull
request, even when it is an urgent fix or rollback due to build breakage.
Skipping pull request bypasses test coverage and could potentially cause the
build to fail, or fail to fix breakage.  In addition, pull requests ensure that
changes are communicated properly and potential flaws or improvements can be
spotted, even after the merge happens.

## Contributor License Agreement

If you are merging a larger contribution, please make sure that the contributor
has an ICLA on file with the Apache Secretary. You can view the list of
committers [here](https://home.apache.org/phonebook.html?unix=committers), as
well as [ICLA-signers who aren’t yet
committers](http://home.apache.org/unlistedclas.html).

For smaller contributions, however, this is not required. In this case, we rely
on [clause five](https://www.apache.org/licenses/LICENSE-2.0#contributions) of
the Apache License, Version 2.0, describing licensing of intentionally
submitted contributions.

## Tests

Before merging, please make sure that Jenkins tests pass, as visible in the
GitHub pull request. Do not merge the pull request if there are test failures.

If the pull request contains changes that call for extra test coverage, you can
ask Jenkins to run an extended test suite. For example, if the pull request
modifies a runner, you can run the full `ValidatesRunner` suite with a comment
such as "Run Spark ValidatesRunner". You can run the examples and some IO
integration tests with "Run Java PostCommit".

## Finishing touches

At some point in the review process, the change to the codebase will be 
complete. However, the pull request may have a collection of review-related
commits that are not meaningful to preserve in the history. The reviewer should
give the LGTM and then request that the author of the pull request rebase,
squash, split, etc, the commits, so that the history is most useful:
* Favor commits that do just one thing. The commit is the smallest unit of easy
rollback; it is easy to roll back many commits, or a whole pull request, but
harder to roll back part of a commit.
* Commit messages should tag JIRAs and be otherwise descriptive.
It should later not be necessary to find a merge or first PR commit to find out what caused a change.
* `CHANGES.md` file should be updated with noteworthy changes (e.g. new features, backward 
incompatible changes, dependency changes, etc.).
* Squash the "Fixup!", "Address comments" type of commits that resulted from review iterations.

## Merging it!

While it is preferred that authors squash commits after review is complete,
there may be situations where it is more practical for the committer to handle this
(such as when the action to be taken is obvious or the author isn't available).
The committer may use the "Squash and merge" option in Github (or modify the PR commits in other ways).
The committer is ultimately responsible and we "trust the committer's judgment"!

After all the tests pass, there should be a green merge button at the bottom of
the pull request. There are multiple choices. Unless you want to squash commits
as part of the merge (see above) you should choose "Merge pull
request" and ensure "Create a merge commit" is selected from the drop down.
This preserves the commit history and adds a merge
commit, so be sure the commit history has been curated appropriately.

Do _not_ use the default GitHub commit message, which looks like this:

    Merge pull request #1234 from some_user/transient_branch_name

    [BEAM-7873] Fix the foo bizzle bazzle

Instead, pull it all into the subject line:

    Merge pull request #1234: [BEAM-7873] Fix the foo bizzle bazzle

If you have comments to add, put them in the body of the commit message.
