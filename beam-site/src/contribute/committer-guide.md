---
layout: section
title: "Beam Committer Guide"
permalink: /contribute/committer-guide/
section_menu: section-menu/contribute.html
---

# Committer Guide

This guide is for
[committers](https://www.apache.org/foundation/how-it-works.html#committers)
and covers Beam's guidelines for reviewing and merging code.

## Always get to LGTM ("Looks good to me!")

After a pull request goes through rounds of reviews and revisions, it will
become ready for merge. A committer (who is _not_ the author of the code)
should signal this either by GitHub "approval" or by a comment such as "Looks
good to me!" (LGTM). Any committer can then merge the pull request. It is fine
for a committer to self-merge if another committer has reviewed the code and
approved it, just be sure to be explicit about whose job it is!

Pull requests should not be merged before the review has received an explicit
LGTM from another committer. Exceptions to this rule are rare and made on a
case-by-case basis. A committer may use their discretion for situations such as
build breaks. In this case, you should still seek a review on the pull request!
A common acronym you may see is "TBR" -- "to be reviewed".

Committers should never commit anything without going through a pull request,
because that bypasses test coverage and could potentially cause the build to
fail. In addition, pull requests ensure that changes are communicated properly
and potential flaws or improvements can be spotted.  **Always go through a pull
request, even if you won’t wait for the code review.** Even then, reviewers
can provide comments in the pull request after the merge happens, for use
in follow-up pull requests.

## Contributor License Agreement

If you are merging a larger contribution, please make sure that the contributor
has an ICLA on file with the Apache Secretary. You can view the list of
committers [here](http://home.apache.org/phonebook.html?unix=committers), as
well as [ICLA-signers who aren’t yet
committers](http://home.apache.org/unlistedclas.html).

For smaller contributions, however, this is not required. In this case, we rely
on [clause five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of
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
squash, split, etc, the commits, so that the history is most useful. Favor
commits that do just one thing. The commit is the smallest unit of easy
rollback; it is easy to roll back many commits, or a whole pull request, but
harder to roll back part of a commit.

## Merging it!

After all the tests pass, there should be a green merge button at the bottom of
the pull request.  There are multiple choices and you should choose "Merge pull
request" (the default). This preserves the commit history and adds a merge
commit, so be sure the commit history has been curated appropriately.

Do _not_ use the default GitHub commit message, which looks like this:

    Merge pull request #1234 from some_user/transient_branch_name

    [BEAM-7873] Fix the foo bizzle bazzle

Instead, pull it all into the subject line:

    Merge pull request #1234: [BEAM-7873] Fix the foo bizzle bazzle

If you have comments to add, put them in the body of the commit message.
