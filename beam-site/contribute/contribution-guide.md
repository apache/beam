---
layout: default
title: "Beam Contribution Guide"
permalink: /contribute/contribution-guide/
redirect_from: /contribution-guide/
---

# Apache Beam Contribution Guide

* TOC
{:toc}

The Apache Beam community welcomes contributions from anyone with a passion for data processing! Beam has many different opportunities for contributions -- write new examples, add new user-facing libraries (new statistical libraries, new IO connectors, etc), work on the core programming model, build specific runners (Apache Flink, Apache Spark, Google Cloud Dataflow, etc), or participate on the documentation effort.

We use a review-then-commit workflow in Beam for all contributions.

![Alt text]({{ "/images/contribution-guide-1.png" | prepend: site.baseurl }} "Workflow image")

**For larger contributions or those that affect multiple components:**

1. **Engage**: We encourage you to work with the Beam community on the [Apache JIRA issue tracker](https://issues.apache.org/jira/browse/BEAM) and [developer’s mailing list](http://beam.incubator.apache.org/mailing_lists/) to identify good areas for contribution.
1. **Design:** More complicated contributions will likely benefit from some early discussion in order to scope and design them well.

**For all contributions:**

1. **Code:** The best part ;-)
1. **Review:** Submit a pull request with your contribution to our [GitHub mirror](https://github.com/apache/incubator-beam/). Work with a committer to review and iterate on the code, if needed.
1. **Commit:** A Beam committer merges the pull request into our [Apache repository](https://git-wip-us.apache.org/repos/asf/incubator-beam.git).

We look forward to working with you!

## Engage

### Mailing list(s)
We discuss design and implementation issues on dev@beam.incubator.apache.org mailing list, which is archived [here](http://mail-archives.apache.org/mod_mbox/incubator-beam-dev/). Join by emailing [`dev-subscribe@beam.incubator.apache.org`](mailto:dev-subscribe@beam.incubator.apache.org).

If interested, you can also join [`user@beam.incubator.apache.org`](http://mail-archives.apache.org/mod_mbox/incubator-beam-user/) and [`commits@beam.incubator.apache.org`](http://mail-archives.apache.org/mod_mbox/incubator-beam-commits/) too.

### Apache JIRA
We use [Apache JIRA](https://issues.apache.org/jira/browse/BEAM) as an issue tracking and project management tool, as well as a way to communicate among a very diverse and distributed set of contributors. To be able to gather feedback, avoid frustration, and avoid duplicated efforts all Beam-related work should be tracked there.

If you do not already have an Apache JIRA account, sign up [here](https://issues.apache.org/jira/).

If a quick [search](https://issues.apache.org/jira/issues/?jql=project%3DBEAM%20AND%20text%20~%20%22the%20thing%20I%20want%20to%20contribute%22) doesn’t turn up an existing JIRA issue for the work you want to contribute, create it. Please discuss your proposal with a committer or the [component lead](https://issues.apache.org/jira/browse/BEAM/?selectedTab=com.atlassian.jira.jira-projects-plugin:components-panel) in JIRA or, alternatively, on the developer mailing list.

If there’s an existing JIRA issue for your intended contribution, please comment about your intended work. Once the work is understood, a committer will assign the issue to you. (If you don’t have a JIRA role yet, you’ll be added to the “contributor” role.) If an issue is currently assigned, please check with the current assignee before reassigning.

For moderate or large contributions, you should not start coding or writing a design doc unless there is a corresponding JIRA issue assigned to you for that work. Simple changes, like fixing typos, do not require an associated issue.

## Design
To avoid potential frustration during the code review cycle, we encourage you to clearly scope and design non-trivial contributions with the Beam community before you start coding.

Generally, the JIRA issue is the best place to gather relevant design docs, comments, or references. It’s great to explicitly include relevant stakeholders early in the conversation. For designs that may be generally interesting, we also encourage conversations on the developer’s mailing list.

We suggest using [Google Docs](https://docs.google.com/) for sharing designs that may benefit from diagrams or comments. Please remember to make the document world-commentable and add a link to it from the relevant JIRA issue. We also track Beam-related documents in [this shared folder](https://drive.google.com/folderview?id=0B-IhJZh9Ab52OFBVZHpsNjc4eXc&usp=sharing).

## Code
To contribute code to Apache Beam, you’ll have to do a few administrative steps once, and then follow a few guidelines for each contribution.

### One-time Setup

#### [Potentially] Submit Contributor License Agreement
Apache Software Foundation (ASF) desires that all contributors of ideas, code, or documentation to the Apache projects complete, sign, and submit an [Individual Contributor License Agreement](https://www.apache.org/licenses/icla.txt) (ICLA). The purpose of this agreement is to clearly define the terms under which intellectual property has been contributed to the ASF and thereby allow us to defend the project should there be a legal dispute regarding the software at some future time.

We require you to have an ICLA on file with the Apache Secretary for larger contributions only. For smaller ones, however, we rely on  [clause five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of the Apache License, Version 2.0, describing licensing of intentionally submitted contributions and do not require an ICLA in that case.

#### Obtain a GitHub account
We use GitHub’s pull request functionality to review proposed code changes.

If you do not already have a personal GitHub account, sign up [here](https://github.com/join).

#### Fork the repository on GitHub
Go to the [Beam GitHub mirror](https://github.com/apache/incubator-beam/) and fork the repository to your own private account. This will be your private workspace for staging changes.

We recommend enabling Travis-CI continuous integration coverage on your private fork in order to easily test your changes before proposing a pull request. Go to [Travis-CI](https://travis-ci.org), log in with your GitHub account, and enable coverage for your repository.

#### Clone the repository locally
You are now ready to create the development environment on your local machine. Feel free to repeat these steps on all machines that you want to use for development.

We assume you are using SSH-based authentication with GitHub. If necessary, exchange SSH keys with GitHub by following [their instructions](https://help.github.com/articles/generating-an-ssh-key/).

Clone Beam’s read-only GitHub mirror.

    $ git clone https://github.com/apache/incubator-beam.git
    $ cd incubator-beam

Add your forked repository as an additional Git remote, where you’ll push your changes.

<pre><code>$ git remote add <b>&lt;GitHub_user&gt;</b> git@github.com:<b>&lt;GitHub_user&gt;</b>/incubator-beam.git</code></pre>

You are now ready to start developing!

### Create a branch in your fork
You’ll work on your contribution in a branch in your own (forked) repository. Create a local branch, initialized with the state of the branch you expect your changes to be merged into. Keep in mind that we use several branches, including `master`, feature-specific, and release-specific branches. If you are unsure, initialize with the state of the `master` branch.

<pre><code>$ git fetch --all
$ git checkout -b <b>&lt;my-branch&gt;</b> origin/master</code></pre>

At this point, you can start making and committing changes to this branch in a standard way.

### Syncing and pushing your branch
Periodically while you work, and certainly before submitting a pull request, you should update your branch with the most recent changes to the target branch.

    $ git pull --rebase

Remember to always use `--rebase` parameter to avoid extraneous merge commits.

To push your local, committed changes to your (forked) repository on GitHub, run:

<pre><code>$ git push <b>&lt;GitHub_user&gt; &lt;my-branch&gt;</b></code></pre>

### Testing
All code should have appropriate unit testing coverage. New code should have new tests in the same contribution. Bug fixes should include a regression test to prevent the issue from reoccurring.

For contributions to the Java code, run unit tests locally via Maven. Alternatively, you can use Travis-CI.

    $ mvn clean verify

## Review
Once the initial code is complete and the tests pass, it’s time to start the code review process. We review and discuss all code, no matter who authors it. It’s a great way to build community, since you can learn from other developers, and they become familiar with your contribution. It also builds a strong project by encouraging a high quality bar and keeping code consistent throughout the project.

### Create a pull request
Organize your commits to make your reviewer’s job easier. Use the following command to re-order, squash, edit, or change description of individual commits.

    $ git rebase -i origin/master

Navigate to the [Beam GitHub mirror](https://github.com/apache/incubator-beam) to create a pull request. The title of the pull request should be strictly in the following format:

<pre><code>[BEAM-<b>&lt;JIRA-issue-#&gt;</b>] Title of the pull request</code></pre>

Please include a descriptive pull request message to help make the reviewer’s job easier. It’s fine to refer to existing design docs or the contents of the associated JIRA as appropriate.

If you know a good committer to review your pull request, please make a comment like the following. If not, don’t worry -- a committer will pick it up.

<pre><code>Hi @<b>&lt;GitHub-reviewer-username&gt;</b>, can you please take a look?</code></pre>

When choosing a reviewer, think about who is the expert on the relevant code, who the stakeholders are for this change, and who else would benefit from becoming familiar with the code. If you’d appreciate comments from additional folks but already have a main reviewer, you can explicitly cc them using <code>@<b>&lt;GitHub-reviewer-username&gt;</b></code>.

### Code Review and Revision
During the code review process, don’t rebase your branch or otherwise modify published commits, since this can remove existing comment history and be confusing to the reviewer. When you make a revision, always push it in a new commit.

Our GitHub mirror automatically provides pre-commit testing coverage using Jenkins and Travis-CI. Please make sure those tests pass; the contribution cannot be merged otherwise.

### LGTM
Once the reviewer is happy with the change, they’ll respond with an LGTM (“*looks good to me!*”). At this point, the committer will take over, possibly make some additional touch ups, and merge your changes into the codebase.

In the case both the author and the reviewer are committers, either can merge the pull request. Just be sure to communicate clearly whose responsibility it is in this particular case.

Thank you for your contribution to Beam!

### Deleting your branch
Once the pull request is merged into the Beam repository, you can safely delete the branch locally and purge it from your forked repository.

From another local branch, run:

<pre><code>$ git fetch --all
$ git branch -d <b>&lt;my-branch&gt;</b>
$ git push <b>&lt;GitHub_user&gt;</b> --delete <b>&lt;my-branch&gt;</b></code></pre>

## Commit (committers only)
Once the code has been peer reviewed by a committer, the next step is for the committer to merge it into the [authoritative Apache repository](https://git-wip-us.apache.org/repos/asf/incubator-beam.git), not the read-only GitHub mirror. (In the case that the author is also a committer, it is acceptable for either the author or reviewer to do the merge. Just be explicit about whose job it is!)

Pull requests should not be merged before the review has received an explicit LGTM from another committer. Exceptions to this rule may be made rarely, on a case-by-case basis only, in the committer’s discretion for situations such as build breakages.

Committers should never commit anything without going through a pull request, since that would bypass test coverage and potentially cause the build to fail due to checkstyle, etc. In addition, pull requests ensure that changes are communicated properly and potential flaws or improvements can be spotted. **Always go through the pull request, even if you won’t wait for the code review.** Even then, comments can be provided in the pull requests after it has been merged to work on follow-ups.

Committing is currently a manual process, but we are investigating tools to automate pieces of this process.

### One-time Setup
Add the Apache Git remote in your local clone, by running:

    $ git remote add apache https://git-wip-us.apache.org/repos/asf/incubator-beam.git

We recommend renaming the `origin` remote to `github`, to avoid confusion when dealing with this many remotes.

    $ git remote rename origin github

For the `github` remote, add an additional fetch reference, which will cause every pull request to be made available as a remote branch in your workspace.

    $ git config --local --add remote.github.fetch \
        '+refs/pull/*/head:refs/remotes/github/pr/*'

You can confirm your configuration by running the following command.

<pre><code>$ git remote -v
apache	https://git-wip-us.apache.org/repos/asf/incubator-beam.git (fetch)
apache	https://git-wip-us.apache.org/repos/asf/incubator-beam.git (push)
github	https://github.com/apache/incubator-beam.git (fetch)
github	https://github.com/apache/incubator-beam.git (push)
<b>&lt;username&gt;</b>	git@github.com:<b>&lt;username&gt;</b>/beam.git (fetch)
<b>&lt;username&gt;</b>	git@github.com:<b>&lt;username&gt;</b>/beam.git (push)</code></pre>

### Contributor License Agreement
If you are merging a larger contribution, please make sure that the contributor has an ICLA on file with the Apache Secretary. You can view the list of committers [here](http://home.apache.org/phonebook.html?unix=committers), as well as [ICLA-signers who aren’t yet committers](http://home.apache.org/unlistedclas.html).

For smaller contributions, however, this is not required. In this case, we rely on [clause five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of the Apache License, Version 2.0, describing licensing of intentionally submitted contributions.

### Tests
Before merging, please make sure both Travis-CI and Jenkins tests pass, as visible in the GitHub pull request. Do not merge the pull request otherwise.

### Finishing touches
At some point in the review process, you should take the pull request over and complete any outstanding work that is either minor, stylistic, or otherwise outside the expertise of the contributor.

Fetch references from all remote repositories, and checkout the specific pull request branch.

<pre>
</code>$ git fetch --all
$ git checkout -b finish-pr-<b>&lt;pull-request-#&gt;</b> github/pr/<b>&lt;pull-request-#&gt;</b></code></pre>

At this point, you can commit any final touches to the pull request. For example, you should:

* Rebase on current state of the target branch.
* Fix typos.
* Reorganize commits that are part of the pull request, such as squash them into fewer commits that make sense for a historical perspective.

You will often need the following command, assuming you’ll be merging changes into the `master` branch:

    $ git rebase -i apache/master

Please make sure to retain authorship of original commits to give proper credit to the contributor. You are welcome to change their commits slightly (e.g., fix a typo) and squash them, but more substantive changes should be a separate commit and review.

### Merge process
Once you are ready to merge, fetch all remotes, checkout the destination branch and merge the changes.

<pre><code>$ git fetch --all
$ git checkout apache/master
$ git merge --no-ff \
&nbsp;&nbsp;&nbsp;&nbsp;-m $'[BEAM-<b>&lt;JIRA-issue-#&gt;</b>] <b>&lt;Title&gt;</b>\n\nThis closes #<b>&lt;pull-request-#&gt;</b>' \
&nbsp;&nbsp;&nbsp;&nbsp;finish-pr-<b>&lt;pull-request-#&gt;</b></code></pre>

Always use `--no-ff` option and the specific commit message "This closes #<b>&lt;pull request #&gt;</b>"" -- it ensures proper marking in the tooling. It would be nice to include additional information in the merge commit message, such as the title and summary of the pull request.

At this point, you want to ensure everything is right. Test it with `mvn verify`. Run `gitk` or `git log --graph,` etc. When you are happy with how it looks, push it. This is the point of no return -- proceed with caution.

    $ git push apache HEAD:master

Done. You can delete the local <code>finish-pr-<b>&lt;pull-request-#&gt;</b></code> branch if you like.

## Additional Projects

### Website
We use the same general review-then-commit process for changes to the Beam website, which uses [this GitHub Mirror](https://github.com/apache/incubator-beam-site). The website uses the [Jekyll](http://jekyllrb.com) framework to make website development easier. The [README file](https://github.com/apache/incubator-beam-site/blob/asf-site/README.md) in the website repository has more information on how to:

* Install Jekyll
* Make changes to the website
* Test your changes

#### Editing the website
You can checkout the website repository with the following commands. This will allow you to edit the website in a local environment provided you have installed [Jekyll](http://jekyllrb.com) and understand how to use it.

<pre><code>git clone -b asf-site https://github.com/apache/incubator-beam-site.git
cd incubator-beam-site
git remote add <b>&lt;GitHub_user&gt;</b> git@github.com:<b>&lt;GitHub_user&gt;</b>/incubator-beam-site.git
git fetch --all
git checkout -b <b>&lt;my-branch&gt;</b> origin/asf-site</code></pre>

#### Committing website changes

Committers can commit website changes with the following commands. **Changes to the website must follow the same process outlined above** for changes to the Apache Beam code base.

<pre><code>git remote add apache https://git-wip-us.apache.org/repos/asf/incubator-beam-site.git
git remote rename origin github
git config --local --add remote.github.fetch \
&nbsp;&nbsp;&nbsp;&nbsp;'+refs/pull/*/head:refs/remotes/github/pr/*'</code></pre>
