---
layout: default
title: "Beam Contribution Guide"
permalink: /contribute/contribution-guide/
redirect_from: /contribution-guide/
---

# Apache Beam Contribution Guide

* TOC
{:toc}

The Apache Beam community welcomes contributions from anyone with a passion for data processing! Beam has many different opportunities for contributions -- write new examples, add new user-facing libraries (new statistical libraries, new IO connectors, etc), work on the core programming model, build specific runners (Apache Apex, Apache Flink, Apache Spark, Google Cloud Dataflow, etc), or participate on the documentation effort.

We use a review-then-commit workflow in Beam for all contributions.

![Alt text]({{ "/images/contribution-guide-1.png" | prepend: site.baseurl }} "Workflow image")

**For larger contributions or those that affect multiple components:**

1. **Engage**: We encourage you to work with the Beam community on the [Apache JIRA issue tracker](https://issues.apache.org/jira/browse/BEAM) and [developer’s mailing list]({{ site.baseurl }}/get-started/support/) to identify good areas for contribution.
1. **Design:** More complicated contributions will likely benefit from some early discussion in order to scope and design them well.

**For all contributions:**

1. **Code:** The best part ;-)
1. **Review:** Submit a pull request with your contribution to our [GitHub mirror](https://github.com/apache/beam/). Work with a committer to review and iterate on the code, if needed.
1. **Commit:** A Beam committer merges the pull request into our [Apache repository](https://git-wip-us.apache.org/repos/asf/beam.git).

We look forward to working with you!

## Engage

### Mailing list(s)
We discuss design and implementation issues on the dev@beam.apache.org mailing list, which is archived [here](https://lists.apache.org/list.html?dev@beam.apache.org). Join by emailing [`dev-subscribe@beam.apache.org`](mailto:dev-subscribe@beam.apache.org).

If interested, you can also join the other [mailing lists]({{ site.baseurl }}/get-started/support/).

### JIRA issue tracker
We use the Apache Software Foundation's [JIRA](https://issues.apache.org/jira/browse/BEAM) as an issue tracking and project management tool, as well as a way to communicate among a very diverse and distributed set of contributors. To be able to gather feedback, avoid frustration, and avoid duplicated efforts all Beam-related work should be tracked there.

If you do not already have an Apache JIRA account, sign up [here](https://issues.apache.org/jira/).

If a quick [search](https://issues.apache.org/jira/issues/?jql=project%3DBEAM%20AND%20text%20~%20%22the%20thing%20I%20want%20to%20contribute%22) doesn’t turn up an existing JIRA issue for the work you want to contribute, create it. Please discuss your idea with a committer or the [component lead](https://issues.apache.org/jira/browse/BEAM/?selectedTab=com.atlassian.jira.jira-projects-plugin:components-panel) in JIRA or, alternatively, on the developer mailing list.

If there’s an existing JIRA issue for your intended contribution, please comment about your intended work. Once the work is understood, a committer will assign the issue to you. (If you don’t have a JIRA role yet, you’ll be added to the “contributor” role.) If an issue is currently assigned, please check with the current assignee before reassigning.

For moderate or large contributions, you should not start coding or writing a design document unless there is a corresponding JIRA issue assigned to you for that work. Simple changes, like fixing typos, do not require an associated issue.

### Online discussions
We don't have an official IRC channel. Most of the online discussions happen in the [Apache Beam Slack channel](https://apachebeam.slack.com/). If you want access, you need to send an email to the user mailing list to [request access](mailto:user@beam.apache.org?subject=Regarding Beam Slack Channel&body=Hello%0D%0A%0ACan someone please add me to the Beam slack channel?%0D%0A%0AThanks.).

Chat rooms are great for quick questions or discussions on specialized topics. Remember that we strongly encourage communication via the mailing lists, and we prefer to discuss more complex subjects by email. Developers should be careful to move or duplicate all the official or useful discussions to the issue tracking system and/or the dev mailing list.

## Design

To avoid potential frustration during the code review cycle, we encourage you to clearly scope and design non-trivial contributions with the Beam community before you start coding.

Generally, the JIRA issue is the best place to gather relevant design docs, comments, or references. It’s great to explicitly include relevant stakeholders early in the conversation. For designs that may be generally interesting, we also encourage conversations on the developer’s mailing list.

We suggest using [Google Docs](https://docs.google.com/) for sharing designs that may benefit from diagrams or comments. Please remember to make the document world-commentable and add a link to it from the relevant JIRA issue. You may want to start from this [template](https://docs.google.com/document/d/1qYQPGtabN5-E4MjHsecqqC7PXvJtXvZukPfLXQ8rHJs/edit?usp=sharing).

## Code
To contribute code to Apache Beam, you’ll have to do a few administrative steps once, and then follow a few guidelines for each contribution.

When developing a new `PTransform`, consult the [PTransform Style Guide]({{ site.baseurl }}/contribute/ptransform-style-guide).

### One-time Setup

#### [Potentially] Submit Contributor License Agreement
Apache Software Foundation (ASF) desires that all contributors of ideas, code, or documentation to the Apache projects complete, sign, and submit an [Individual Contributor License Agreement](https://www.apache.org/licenses/icla.pdf) (ICLA). The purpose of this agreement is to clearly define the terms under which intellectual property has been contributed to the ASF and thereby allow us to defend the project should there be a legal dispute regarding the software at some future time.

We require you to have an ICLA on file with the Apache Secretary for larger contributions only. For smaller ones, however, we rely on [clause five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of the Apache License, Version 2.0, describing licensing of intentionally submitted contributions and do not require an ICLA in that case.

#### Obtain a GitHub account
We use GitHub’s pull request functionality to review proposed code changes.

If you do not already have a personal GitHub account, sign up [here](https://github.com/join).

#### Fork the repository on GitHub
Go to the [Beam GitHub mirror](https://github.com/apache/beam/) and fork the repository to your own private account. This will be your private workspace for staging changes.

#### Clone the repository locally
You are now ready to create the development environment on your local machine. Feel free to repeat these steps on all machines that you want to use for development.

We assume you are using SSH-based authentication with GitHub. If necessary, exchange SSH keys with GitHub by following [their instructions](https://help.github.com/articles/generating-an-ssh-key/).

Clone Beam’s read-only GitHub mirror.

    $ git clone https://github.com/apache/beam.git
    $ cd beam

Add your forked repository as an additional Git remote, where you’ll push your changes.

	$ git remote add <GitHub_user> git@github.com:<GitHub_user>/beam.git

You are now ready to start developing!

#### [Python SDK Only] Set up a virtual environemt

We recommend setting up a virtual envioment for developing Python SDK. Please see instructions available in [Quickstart (Python)]({{ site.baseurl }}/get-started/quickstart-py/) for setting up a virtual environment.

#### [Optional] IDE Setup

Depending on your preferred development environment, you may need to prepare it to develop Beam code.

##### IntelliJ

###### Enable Annotation Processing
To configure annotation processing in IntelliJ:

1. Open Annotation Processors Settings dialog box by going to Settings -> Build, Execution, Deployment -> Compiler -> Annotation Processors.
1. Select the following buttons:
   * "Enable annotation processing"
   * "Obtain processors from project classpath"
   * "Store generated sources relative to: _Module content root_"
1. Set the generated source directories to be equal to the Maven directories:
   * Set "Production sources directory:" to "target/generated-sources/annotations".
   * Set "Test sources directory:" to "target/generated-test-sources/test-annotations".
1. Click "OK".

###### Checkstyle
IntelliJ supports checkstyle within the IDE using the Checkstyle-IDEA plugin.

1. Install the "Checkstyle-IDEA" plugin from the IntelliJ plugin repository.
1. Configure the plugin by going to Settings -> Other Settings -> Checkstyle.
1. Set the "Scan Scope" to "Only Java sources (including tests)".
1. In the "Configuration File" pane, add a new configuration using the plus icon:
    1. Set the "Description" to "Beam".
    1. Select "Use a local Checkstyle file", and point it to
      "sdks/java/build-tools/src/main/resources/beam/checkstyle.xml" within
      your repository.
    1. Check the box for "Store relative to project location", and click
      "Next".
    1. Configure the "checkstyle.suppressions.file" property value to
      "suppressions.xml", and click "Next", then "Finish".
1. Select "Beam" as the only active configuration file, and click "Apply" and
   "OK".
1. Checkstyle will now give warnings in the editor for any Checkstyle
   violations.

You can also scan an entire module by opening the Checkstyle tools window and
clicking the "Check Module" button. The scan should report no errors.

Note: Selecting "Check Project" may report some errors from the archetype
modules as they are not configured for Checkstyle validation.

###### Code Style
IntelliJ supports code styles within the IDE. Use one of the following to ensure your code style
matches the project's checkstyle enforcements.

1. (Option 1) Configure IntelliJ to use "beam-codestyle.xml".
    1. Go to Settings -> Code Style -> Java.
    1. Click the cogwheel icon next to 'Scheme' and select Import Scheme -> Eclipse XML Profile.
    1. Select "sdks/java/build-tools/src/main/resources/beam/beam-codestyle.xml".
    1. Click "OK".
    1. Click "Apply" and "OK".
1. (Option 2) Install [Google Java Format plugin](https://plugins.jetbrains.com/plugin/8527-google-java-format).

##### Eclipse

Use a recent Eclipse version that includes m2e. Currently we recommend Eclipse Neon.
Start Eclipse with a fresh workspace in a separate directory from your checkout.

###### Initial setup
1. Install m2e-apt: Beam uses apt annotation processing to provide auto generated code. One example is the usage of [Google AutoValue](https://github.com/google/auto/tree/master/value). By default m2e does not support this and you will see compile errors.

	Help
	-> Eclipse Marketplace
	-> Search for "m2 apt"
	-> Install m2e-apt 1.2 or higher

1. Activate the apt processing

	Window
	-> Preferences
	-> Maven
	-> Annotation processing
	-> Switch to Experimental: Delegate annotation processing ...
	-> Ok

1. Import the beam projects

	File
	-> Import...
	-> Existing Maven Projects
	-> Browse to the directory you cloned into and select "beam"
	-> make sure all beam projects are selected
	-> Finalize

You now should have all the beam projects imported into eclipse and should see no compile errors.

###### Checkstyle
Eclipse supports checkstyle within the IDE using the Checkstyle plugin.

1. Install the [Checkstyle plugin](https://marketplace.eclipse.org/content/checkstyle-plug).
1. Configure Checkstyle plugin by going to Preferences - Checkstyle.
    1. Click "New...".
    1. Select "External Configuration File" for type.
    1. Click "Browse..." and select "sdks/java/build-tools/src/main/resources/beam/checkstyle.xml".
    1. Enter "Beam Checks" under "Name:".
    1. Click "OK", then "OK".

###### Code Style
Eclipse supports code styles within the IDE. Use one of the following to ensure your code style
matches the project's checkstyle enforcements.

1. (Option 1) Configure Eclipse to use "beam-codestyle.xml".
    1. Go to Preferences -> Java -> Code Style -> Formatter.
    1. Click "Import..." and select "sdks/java/build-tools/src/main/resources/beam/beam-codestyle.xml".
    1. Click "Apply" and "OK".
1. (Option 2) Install [Google Java Format plugin](https://github.com/google/google-java-format#eclipse).

### Create a branch in your fork
You’ll work on your contribution in a branch in your own (forked) repository. Create a local branch, initialized with the state of the branch you expect your changes to be merged into. Keep in mind that we use several branches, including `master`, feature-specific, and release-specific branches. If you are unsure, initialize with the state of the `master` branch.

	$ git fetch --all
	$ git checkout -b <my-branch> origin/master

At this point, you can start making and committing changes to this branch in a standard way.

### Syncing and pushing your branch
Periodically while you work, and certainly before submitting a pull request, you should update your branch with the most recent changes to the target branch.

    $ git pull --rebase

Remember to always use `--rebase` parameter to avoid extraneous merge commits.

Then you can push your local, committed changes to your (forked) repository on GitHub. Since rebase may change that branch's history, you may need to force push. You'll run:

	$ git push <GitHub_user> <my-branch> --force

### Testing
All code should have appropriate unit testing coverage. New code should have new tests in the same contribution. Bug fixes should include a regression test to prevent the issue from reoccurring.

#### Java SDK

For contributions to the Java code, run unit tests locally via Maven.

    $ mvn clean verify

#### Python SDK

For contributions to the Python code, you can use command given below to run unit tests locally. If you update any of the [cythonized](http://cython.org) files in Python SDK, you must install "cython" package before running following command to properly test your code. We recommend setting up a virtual environment before testing your code.

    $ python setup.py test

You can use following command to run a single test method.

    $ python setup.py test -s <module>.<test class>.<test method>

To Check for lint errors locally, install "tox" package and run following command.

    $ pip install tox
    $ tox -e lint

Beam supports running Python SDK tests using Maven. For this, navigate to root directory of your Apache Beam clone and execute following command. Currently this cannot be run from a virtual environment.

    $ mvn clean verify -pl sdks/python

## Review
Once the initial code is complete and the tests pass, it’s time to start the code review process. We review and discuss all code, no matter who authors it. It’s a great way to build community, since you can learn from other developers, and they become familiar with your contribution. It also builds a strong project by encouraging a high quality bar and keeping code consistent throughout the project.

### Create a pull request
Organize your commits to make a committer’s job easier when reviewing. Committers normally prefer multiple small pull requests, instead of a single large pull request. Within a pull request, a relatively small number of commits that break the problem into logical steps is preferred. For most pull requests, you'll squash your changes down to 1 commit. You can use the following command to re-order, squash, edit, or change description of individual commits.

    $ git rebase -i origin/master

You'll then push to your branch on GitHub. Note: when updating your commit after pull request feedback and use squash to get back to one commit, you will need to do a force submit to the branch on your repo.

Navigate to the [Beam GitHub mirror](https://github.com/apache/beam) to create a pull request. The title of the pull request should be strictly in the following format:

	[BEAM-<JIRA-issue-#>] <Title of the pull request>

Please include a descriptive pull request message to help make the comitter’s job easier when reviewing. It’s fine to refer to existing design docs or the contents of the associated JIRA as appropriate.

If you know a good committer to review your pull request, please make a comment like the following. If not, don’t worry -- a committer will pick it up.

	Hi @<GitHub-committer-username>, can you please take a look?

When choosing a committer to review, think about who is the expert on the relevant code, who the stakeholders are for this change, and who else would benefit from becoming familiar with the code. If you’d appreciate comments from additional folks but already have a main committer, you can explicitly cc them using `@<GitHub-committer-username>`.

### Code Review and Revision
During the code review process, don’t rebase your branch or otherwise modify published commits, since this can remove existing comment history and be confusing to the committer when reviewing. When you make a revision, always push it in a new commit.

Our GitHub mirror automatically provides pre-commit testing coverage using Jenkins. Please make sure those tests pass; the contribution cannot be merged otherwise.

### LGTM
Once the committer is happy with the change, they’ll respond with an LGTM (“*looks good to me!*”). At this point, the committer will take over, possibly make some additional touch ups, and merge your changes into the codebase.

In the case the author is also a committer, either can merge the pull request. Just be sure to communicate clearly whose responsibility it is in this particular case.

Thank you for your contribution to Beam!

### Deleting your branch
Once the pull request is merged into the Beam repository, you can safely delete the branch locally and purge it from your forked repository.

From another local branch, run:

	$ git fetch --all
	$ git branch -d <my-branch>
	$ git push <GitHub_user> --delete <my-branch>

## Commit (committers only)
Once the code has been peer reviewed by a committer, the next step is for the committer to merge it into the [authoritative Apache repository](https://git-wip-us.apache.org/repos/asf/beam.git), not the read-only GitHub mirror. (In the case that the author is also a committer, it is acceptable for either the author of the change or committer who reviewed the change to do the merge. Just be explicit about whose job it is!)

Pull requests should not be merged before the review has received an explicit LGTM from another committer. Exceptions to this rule may be made rarely, on a case-by-case basis only, in the committer’s discretion for situations such as build breakages.

Committers should never commit anything without going through a pull request, since that would bypass test coverage and potentially cause the build to fail due to checkstyle, etc. In addition, pull requests ensure that changes are communicated properly and potential flaws or improvements can be spotted. **Always go through the pull request, even if you won’t wait for the code review.** Even then, comments can be provided in the pull requests after it has been merged to work on follow-ups.

Committing is currently a manual process, but we are investigating tools to automate pieces of this process.

### One-time Setup
Add the Apache Git remote in your local clone, by running:

    $ git remote add apache https://git-wip-us.apache.org/repos/asf/beam.git

We recommend renaming the `origin` remote to `github`, to avoid confusion when dealing with this many remotes.

    $ git remote rename origin github

For the `github` remote, add an additional fetch reference, which will cause every pull request to be made available as a remote branch in your workspace.

    $ git config --local --add remote.github.fetch \
        '+refs/pull/*/head:refs/remotes/github/pr/*'

You can confirm your configuration by running the following command.

	$ git remote -v
	apache	https://git-wip-us.apache.org/repos/asf/beam.git (fetch)
	apache	https://git-wip-us.apache.org/repos/asf/beam.git (push)
	github	https://github.com/apache/beam.git (fetch)
	github	https://github.com/apache/beam.git (push)
	<username>	git@github.com:<username>/beam.git (fetch)
	<username>	git@github.com:<username>/beam.git (push)

### Contributor License Agreement
If you are merging a larger contribution, please make sure that the contributor has an ICLA on file with the Apache Secretary. You can view the list of committers [here](http://home.apache.org/phonebook.html?unix=committers), as well as [ICLA-signers who aren’t yet committers](http://home.apache.org/unlistedclas.html).

For smaller contributions, however, this is not required. In this case, we rely on [clause five](http://www.apache.org/licenses/LICENSE-2.0#contributions) of the Apache License, Version 2.0, describing licensing of intentionally submitted contributions.

### Tests
Before merging, please make sure that Jenkins tests pass, as visible in the GitHub pull request. Do not merge the pull request otherwise.

### Finishing touches
At some point in the review process, you should take the pull request over and complete any outstanding work that is either minor, stylistic, or otherwise outside the expertise of the contributor.

Fetch references from all remote repositories, and checkout the specific pull request branch.

	$ git fetch --all
	$ git checkout -b finish-pr-<pull-request-#> github/pr/<pull-request-#>

At this point, you can commit any final touches to the pull request. For example, you should:

* Rebase on current state of the target branch.
* Fix typos.
* Reorganize commits that are part of the pull request, such as squash them into fewer commits that make sense for a historical perspective.

You will often need the following command, assuming you’ll be merging changes into the `master` branch:

    $ git rebase -i apache/master

Please make sure to retain authorship of original commits to give proper credit to the contributor. You are welcome to change their commits slightly (e.g., fix a typo) and squash them, but more substantive changes should be a separate commit and review.

### Merge process
Once you are ready to merge, fetch all remotes, checkout the destination branch and merge the changes.

	$ git fetch --all
	$ git checkout apache/master
	$ git merge --no-ff -m 'This closes #<pull-request-#>' finish-pr-<pull-request-#>

Always use `--no-ff` option and the specific commit message "This closes #<pull request #>" -- it ensures proper marking in the tooling. It would be nice to include additional information in the merge commit message, such as the title and summary of the pull request.

At this point, you want to ensure everything is right. Test it with `mvn verify`. Run `gitk` or `git log --graph,` etc. When you are happy with how it looks, push it. This is the point of no return -- proceed with caution.

    $ git push apache HEAD:master

Done. You can delete the local `finish-pr-<pull-request-#>` branch if you like.

## Granting more rights to a contributor

The project management committee (PMC) can grant more rights to a contributor, such as commit access or decision power, and recognize them as new [committers or PMC members]({{ site.baseurl }}/contribute/team/).

The PMC periodically discusses this topic and privately votes to grant more rights to a contributor. If the vote passes, the contributor is invited to accept or reject the nomination. Once accepted, the PMC announces the decision publicly and updates the list of team member accordingly.

The key to the selection process is [Meritocracy](http://apache.org/foundation/how-it-works.html#meritocracy), literally government by merit. Contributors earn merit in many ways: contributing code, testing releases, participating in documentation effort, answering user questions, debating design proposals, triaging issues, evangelizing the project, growing user base, and any other action that benefits the project as a whole.

Therefore, there isn’t a single committer bar, e.g., a specific number of commits. In most cases, new committers will have a combination of different types of contributions that are hard to compare to each other. Similarly, there isn’t a single PMC bar either. The PMC discusses all contributions from an individual, and
evaluates the overall impact across all the dimensions above.

Nothing gives us greater joy than recognizing new committers or PMC members -- that's the only way we can grow. If there’s ever any doubt about this topic, please email dev@ or private@ and we’ll gladly discuss.

## Special Cases

The directions above assume you are submitting code to the `beam` repository's `master` branch. In addition, there are a few other locations where code is maintained. Generally these follow the same *engage*-*design*-**code**-**review**-**commit** process as above, with some minor adjustments to commands.

### Feature Branches

Some larger features are developed on a feature branch before being merged into `master`. In particular, this is often used for initial development of new components like SDKs or runners.

#### Developing

To contribute code on a feature branch, use the same process as above, but replace `master` with the [name of the branch]({{ site.baseurl }}/contribute/work-in-progress/#feature-branches).

Since feature branches are often used for new components, you may find that
there is no [committer]({{ site.baseurl }}/contribute/team/) familiar with all the details of the
new language or runner. In that case, consider asking someone else familiar
with the technology to do an initial review before looping in a committer for a
final review and merge.

If you are working on a feature branch, you'll also want to frequently merge in
changes from `master` in order to prevent life on the branch from deviating too
far from reality.  Like all changes, this should be done via pull request. It
is permitted for a committer to self-merge such a pull request if there are no
conflicts or test failures. If there are any conflicts of tests that need
fixing, then those should get a full review from another committer.

#### Merging into Master

In order for a feature branch to be merged into `master`, new components and major features should aim to meet the following guidelines.

1. Have at least 2 contributors interested in maintaining it, and 1 committer interested in supporting it
1. Provide both end-user and developer-facing documentation
1. Have at least a basic level of unit test coverage
1. Run all existing applicable integration tests with other Beam components and create additional tests as appropriate

Additionally, ...

A new runner should:

1. Be able to handle a subset of the model that address a significant set of use cases (aka. ‘traditional batch’ or ‘processing time streaming’)
1. Update the capability matrix with the current status
1. Add a webpage under `documentation/runners`

A new SDK should:

1. Provide the ability to construct graphs with all the basic building blocks of the model (ParDo, GroupByKey, Window, Trigger, etc)
1. Begin fleshing out the common composite transforms (Count, Join, etc) and IO connectors (Text, Kafka, etc)
1. Have at least one runner that can execute the complete model (may be a direct runner)
1. Provide integration tests for executing against current and future runners
1. Add a webpage under `documentation/sdks`


### Website

The Beam website is in the [Beam Site GitHub mirror](https://github.com/apache/beam-site) repository in the `asf-site` branch (_not_ `master`).

Issues are tracked in the [website](https://issues.apache.org/jira/browse/BEAM/component/12328906) component in JIRA.

#### One-time Setup

The [README file](https://github.com/apache/beam-site/blob/asf-site/README.md) in the website repository has more information on how to set up the required dependencies for your development environment.

The general guidelines for cloning a repository can be adjusted to use the `asf-site` branch of `beam-site`:

	$ git clone -b asf-site https://github.com/apache/beam-site.git
	$ cd beam-site
	$ git remote add <GitHub_user> git@github.com:<GitHub_user>/beam-site.git
	$ git fetch --all
	$ git checkout -b <my-branch> origin/asf-site

#### Working on your change

While you are working on your pull request, you can test and develop live by running the following command in the root folder of the website:

	$ bundle exec jekyll serve --incremental

Jekyll will start a webserver on port 4000. As you make changes to the content, Jekyll will rebuild it automatically.

In addition, you can run the tests to valid your links using:

	$ bundle exec rake test

Both of these commands will cause the `content/` directory to be generated. Merging autogenerated content can get tricky, so please leave this directory out of your commits and pull request by doing:

	$ git checkout -- content

When you are ready, submit a pull request using the [Beam Site GitHub mirror](https://github.com/apache/beam-site), including the JIRA issue as usual.

During review, committers will patch in your PR, generate the static `content/`, and review the changes.

#### Committing website changes (committers only)

Follow the same committer process as above, but using repository `apache/beam-site` and branch `asf-site`.

In addition, the committer is responsible for doing the final `bundle exec jekyll build` to generate the static content, so follow the instructions above to install `jekyll`.

This command generates the `content/` directory. The committer should add and commit the content related to the PR.

	$ git add content/<files related to the pr>
	$ git commit -m "Regenerate website"

Finally you should merge the changes into the `asf-site` branch and push them into the `apache` repository.
