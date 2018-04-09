---
layout: section
title: "Beam Contribution Guide"
permalink: /contribute/contribution-guide/
section_menu: section-menu/contribute.html
redirect_from: /contribution-guide/
---

# Apache Beam Contribution Guide

The Apache Beam community welcomes contributions from anyone with a passion for
data processing! Beam has many different opportunities for contributions --
write new examples, add new user-facing libraries (new statistical libraries,
new IO connectors, etc), work on the core programming model, build specific
runners (Apache Apex, Apache Flink, Apache Spark, Google Cloud Dataflow, etc),
or participate on the documentation effort.

We use a review-then-commit workflow in Beam for all contributions.

![The Beam contribution workflow has 5 steps: engage, design, code, review, and commit.](
  {{ "/images/contribution-guide-1.png" | prepend: site.baseurl }})

**For larger contributions or those that affect multiple components:**

1. **Engage**: We encourage you to work with the Beam community on the [Apache
   JIRA issue tracker](https://issues.apache.org/jira/browse/BEAM) and
   [developer’s mailing list]({{ site.baseurl }}/get-started/support/) to identify
   good areas for contribution.
2. **Design:** More complicated contributions will likely benefit from some
   early discussion in order to scope and design them well.

**For all contributions:**

1. **Code:** The best part ;-)
2. **Review:** Submit a pull request with your contribution to our [GitHub
   mirror](https://github.com/apache/beam/). Work with a committer to review
and iterate on the code, if needed.
3. **Commit:** A Beam committer merges the pull request into our [Apache
   repository](https://gitbox.apache.org/repos/asf/beam.git).

We look forward to working with you!

## Engage

### Mailing list(s)

We discuss design and implementation issues on the `dev@beam.apache.org`
mailing list, which is archived
[here](https://lists.apache.org/list.html?dev@beam.apache.org). Join by
emailing
[`dev-subscribe@beam.apache.org`](mailto:dev-subscribe@beam.apache.org).

If interested, you can also join the other [mailing lists]({{ site.baseurl
}}/get-started/support/).

### JIRA issue tracker

We use the Apache Software Foundation's
[JIRA](https://issues.apache.org/jira/browse/BEAM) as an issue tracking and
project management tool, as well as a way to communicate among a very diverse
and distributed set of contributors. To be able to gather feedback, avoid
frustration, and avoid duplicated efforts all Beam-related work should be
tracked there.

If you do not already have an Apache JIRA account, sign up
[here](https://issues.apache.org/jira/).

If a quick
[search](https://issues.apache.org/jira/issues/?jql=project%3DBEAM%20AND%20text%20~%20%22the%20thing%20I%20want%20to%20contribute%22)
doesn’t turn up an existing JIRA issue for the work you want to contribute,
create it. Please discuss your idea with a committer or the [component
lead](https://issues.apache.org/jira/browse/BEAM/?selectedTab=com.atlassian.jira.jira-projects-plugin:components-panel)
in JIRA or, alternatively, on the developer mailing list.

If there’s an existing JIRA issue for your intended contribution, please
comment about your intended work. Once the work is understood, a committer will
assign the issue to you. (If you don’t have a JIRA role yet, you’ll be added to
the “contributor” role.) If an issue is currently assigned, please check with
the current assignee before reassigning.

For moderate or large contributions, you should not start coding or writing a
design document unless there is a corresponding JIRA issue assigned to you for
that work. Simple changes, like fixing typos, do not require an associated
issue.

### Online discussions

We don't have an official IRC channel. Most of the online discussions happen in
the [Apache Beam Slack channel](https://apachebeam.slack.com/). If you want
access, you need to send an email to the user mailing list to [request
access](mailto:user@beam.apache.org?subject=Regarding Beam Slack
Channel&body=Hello%0D%0A%0ACan someone please add me to the Beam slack
channel?%0D%0A%0AThanks.).

Chat rooms are great for quick questions or discussions on specialized topics.
Remember that we strongly encourage communication via the mailing lists, and we
prefer to discuss more complex subjects by email. Developers should be careful
to move or duplicate all the official or useful discussions to the issue
tracking system and/or the dev mailing list.

## Design

To avoid potential frustration during the code review cycle, we encourage you
to clearly scope and design non-trivial contributions with the Beam community
before you start coding.

Generally, the JIRA issue is the best place to gather relevant design docs,
comments, or references. It’s great to explicitly include relevant stakeholders
early in the conversation. For designs that may be generally interesting, we
also encourage conversations on the developer’s mailing list.

We suggest using [Google Docs](https://docs.google.com/) for sharing designs
that may benefit from diagrams or comments. Please remember to make the
document world-commentable and add a link to it from the relevant JIRA issue.
You may want to start from this
[template](https://docs.google.com/document/d/1qYQPGtabN5-E4MjHsecqqC7PXvJtXvZukPfLXQ8rHJs/edit?usp=sharing).

## Code

To contribute code to Apache Beam, you’ll have to do a few administrative steps
once, and then follow a few guidelines for each contribution.

When developing a new `PTransform`, consult the [PTransform Style Guide]({{
site.baseurl }}/contribute/ptransform-style-guide).

### One-time Setup

#### [Potentially] Submit Contributor License Agreement

Apache Software Foundation (ASF) desires that all contributors of ideas, code,
or documentation to the Apache projects complete, sign, and submit an
[Individual Contributor License
Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA). The purpose of
this agreement is to clearly define the terms under which intellectual property
has been contributed to the ASF and thereby allow us to defend the project
should there be a legal dispute regarding the software at some future time.

We require you to have an ICLA on file with the Apache Secretary for larger
contributions only. For smaller ones, however, we rely on [clause
five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of the Apache
License, Version 2.0, describing licensing of intentionally submitted
contributions and do not require an ICLA in that case.

#### Obtain a GitHub account

We use GitHub’s pull request functionality to review proposed code changes.

If you do not already have a personal GitHub account, sign up
[here](https://github.com/join).

#### Fork the repository on GitHub

Go to the [Beam GitHub mirror](https://github.com/apache/beam/) and fork the
repository to your own private account. This will be your private workspace for
staging changes.

#### Clone the repository locally

You are now ready to create the development environment on your local machine.
Feel free to repeat these steps on all machines that you want to use for
development.

We assume you are using SSH-based authentication with GitHub. If necessary,
exchange SSH keys with GitHub by following [their
instructions](https://help.github.com/articles/generating-an-ssh-key/).

Clone Beam’s read-only GitHub mirror.

    $ git clone https://github.com/apache/beam.git
    $ cd beam

Add your forked repository as an additional Git remote, where you’ll push your
changes.

	$ git remote add <GitHub_user> git@github.com:<GitHub_user>/beam.git

You are now ready to start developing!

#### [Python SDK Only] Set up a virtual environemt

We recommend setting up a virtual envioment for developing Python SDK. Please
see instructions available in [Quickstart (Python)]({{ site.baseurl
}}/get-started/quickstart-py/) for setting up a virtual environment.

#### [Optional] IDE Setup

Depending on your preferred development environment, you may need to prepare it
to develop Beam code. We have some collected tips for [IntelliJ]({{ site.baseurl }}/contribute/intellij)
and [Eclipse]({{ site.baseurl }}/contribute/eclipse).

### Create a branch in your fork

You’ll work on your contribution in a branch in your own (forked) repository.
Create a local branch, initialized with the state of the branch you expect your
changes to be merged into. Keep in mind that we use several branches, including
`master`, feature-specific, and release-specific branches. If you are unsure,
initialize with the state of the `master` branch.

	$ git fetch --all
	$ git checkout -b <my-branch> origin/master

At this point, you can start making and committing changes to this branch in a
standard way.

### Syncing and pushing your branch

Periodically while you work, and certainly before submitting a pull request,
you should update your branch with the most recent changes to the target
branch.

    $ git pull --rebase

Remember to always use `--rebase` parameter to avoid extraneous merge commits.

Then you can push your local, committed changes to your (forked) repository on
GitHub. Since rebase may change that branch's history, you may need to force
push. You'll run:

	$ git push <GitHub_user> <my-branch> --force

### Building

#### Python SDK

Before testing SDK code changes remotely, you must build the Beam tarball. From
the root of the git repository, run:

```
cd sdks/python/
python setup.py sdist
```

Pass the `--sdk_location` flag to use the newly built version. For example:

```
python setup.py sdist > /dev/null && \
    python -m apache_beam.examples.wordcount ... \
        --sdk_location dist/apache-beam-2.5.0.dev0.tar.gz
```

### Testing

All code should have appropriate unit testing coverage. New code should have
new tests in the same contribution. Bug fixes should include a regression test
to prevent the issue from reoccurring.

#### Java SDK

For contributions to the Java code, run checks locally via Gradle.

    $ ./gradlew clean && ./gradlew :beam-sdks-java-core:check

#### Python SDK

For contributions to the Python code, you can use command given below to run
unit tests locally. If you update any of the [cythonized](http://cython.org)
files in Python SDK, you must install "cython" package before running following
command to properly test your code. We recommend setting up a virtual
environment before testing your code.

    $ python setup.py test

You can use following command to run a single test method.

    $ python setup.py test -s <module>.<test class>.<test method>

To Check for lint errors locally, install "tox" package and run following
command.

    $ pip install tox
    $ tox -e lint_py2,lint_py3


Beam supports running Python SDK tests using Gradle. For this, navigate to root
directory of your Apache Beam clone and execute following command. Currently
this cannot be run from a virtual environment.

    $ ./gradlew clean && ./gradlew :beam-sdks-python:check

## Review

Once the initial code is complete and the tests pass, it’s time to start the
code review process. We review and discuss all code, no matter who authors it.
It’s a great way to build community, since you can learn from other developers,
and they become familiar with your contribution. It also builds a strong
project by encouraging a high quality bar and keeping code consistent
throughout the project.

### Create a pull request

Organize your commits to make a committer’s job easier when reviewing.
Committers normally prefer multiple small pull requests, instead of a single
large pull request. Within a pull request, a relatively small number of commits
that break the problem into logical steps is preferred. For most pull requests,
you'll squash your changes down to 1 commit. You can use the following command
to re-order, squash, edit, or change description of individual commits.

    $ git rebase -i origin/master

You'll then push to your branch on GitHub. Note: when updating your commit
after pull request feedback and use squash to get back to one commit, you will
need to do a force submit to the branch on your repo.

Navigate to the [Beam GitHub mirror](https://github.com/apache/beam) to create
a pull request. The title of the pull request should be strictly in the
following format:

	[BEAM-<JIRA-issue-#>] <Title of the pull request>

Please include a descriptive pull request message to help make the comitter’s
job easier when reviewing. It’s fine to refer to existing design docs or the
contents of the associated JIRA as appropriate.

If you know a good committer to review your pull request, please make a comment
like the following. If not, don’t worry -- a committer will pick it up.

	Hi @<GitHub-committer-username>, can you please take a look?

When choosing a committer to review, think about who is the expert on the
relevant code, who the stakeholders are for this change, and who else would
benefit from becoming familiar with the code. If you’d appreciate comments from
additional folks but already have a main committer, you can explicitly cc them
using `@<GitHub-committer-username>`.

### Code Review and Revision

During the code review process, be careful about whether and when to rebase
your branch or otherwise modify published commits, since this can remove
existing comment history and be confusing to the committer when reviewing. When
you make a revision, always push it in a new commit.

Our GitHub mirror automatically provides pre-commit testing coverage using
Jenkins. Please make sure those tests pass; the contribution cannot be merged
otherwise.

### LGTM

Once the committer is happy with the change, they’ll respond with an LGTM
(“*looks good to me!*”). At this point, the committer may ask you to tidy
up the commit history before they merge your changes into the codebase.
This makes it easy for community to look through the history and understand how
something came about, look for a candidate commit to roll back, etc.

In the case the author is also a committer, either can merge the pull request.
Just be sure to communicate clearly whose responsibility it is in this
particular case.

Thank you for your contribution to Beam!

### Deleting your branch

Once the pull request is merged into the Beam repository, you can safely delete
the branch locally and purge it from your forked repository.

From another local branch, run:

	$ git fetch --all
	$ git branch -d <my-branch>
	$ git push <GitHub_user> --delete <my-branch>

### Stale pull requests

The community will close stale pull requests in order to keep the project
healthy. A pull request becomes stale after its author fails to respond to
actionable comments for 60 days.  Author of a closed pull request is welcome to
reopen the same pull request again in the future. The associated JIRAs will be
unassigned from the author but will stay open.

## Granting more rights to a contributor

The project management committee (PMC) can grant more rights to a contributor,
such as commit access or decision power, and recognize them as new [committers
or PMC members]({{ site.baseurl }}/contribute/team/).

The PMC periodically discusses this topic and privately votes to grant more
rights to a contributor. If the vote passes, the contributor is invited to
accept or reject the nomination. Once accepted, the PMC announces the decision
publicly and updates the list of team member accordingly.

The key to the selection process is
[Meritocracy](http://apache.org/foundation/how-it-works.html#meritocracy),
literally government by merit. Contributors earn merit in many ways:
contributing code, testing releases, participating in documentation effort,
answering user questions, debating design proposals, triaging issues,
evangelizing the project, growing user base, and any other action that benefits
the project as a whole.

Therefore, there isn’t a single committer bar, e.g., a specific number of
commits. In most cases, new committers will have a combination of different
types of contributions that are hard to compare to each other. Similarly, there
isn’t a single PMC bar either. The PMC discusses all contributions from an
individual, and evaluates the overall impact across all the dimensions above.

Nothing gives us greater joy than recognizing new committers or PMC members --
that's the only way we can grow. If there’s ever any doubt about this topic,
please email dev@ or private@ and we’ll gladly discuss.

