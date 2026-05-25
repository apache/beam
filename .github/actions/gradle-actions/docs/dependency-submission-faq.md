# Dependency submission FAQ

Implementing a `dependency-submission` workflow for your repository is documented in the
[core documentation](dependency-submission.md). 
But getting it working is the easy part: the dependency alerts you recieve can be confusing and surprising.
Here are some common questions answered.

### How can I easily try this out without experimenting on my main repository?
The https://github.com/gradle/github-dependency-submission-demo repository is setup as a tutorial for you to fork and play with.

### How can I tell if the `dependency-submission` action is working?
Inspect the Dependency Graph for your project (Insights -> Dependency Graph). You should see some dependencies annotated with "Detected by GitHub Dependency Graph Gradle Plugin"

### Why is `(Maven)` stated for all dependencies submitted by this action? I'm not using Maven.
This simply indicates that the dependency was resolved from a standard Gradle/Maven artifact repository. It does not imply which build tool is used.

### Why is every dependency attributed to `settings.gradle.kts`? 
All dependendies detected by the `dependency-submission` action are attributed to the Gradle project as a whole. We found that the best way is to link to the project `Settings` file.
We do not currently attempt to attribute dependencies to the actual file where they were declared.

### Why aren't dependencies be linked to the source file where they are declared?
There are a couple of reasons for this:
1. Gradle doesn't currently provide a mechanism to determine the location where a dependency is declared. In fact, the resulting dependency version can be influenced by many different sources within a Gradle project.
2. The GitHub Dependency Graph was modelled heavily on NPM and doesn't really map well to having multiple source locations for a single dependency declaration. 

We have long-term plans to improve the first point, and we are working with GitHub to resolve the second. However, at this stage the behaviour your are experiencing is what is expected.

### My repository dependency graph contains a dependency that isn't anywhere in my build. Why is the `dependency-submission` action reporting dependencies I'm not using?
If you see a particular dependency version reported in the dependency graph, it means your build is resolving that dependency at some point. 
You may be surprised what transitive dependencies are brought in by declared dependencies and applied plugins in your build.
[See here for a HOW-TO](dependency-submission.md#resolving-a-dependency-vulnerability) on getting the bottom of why the dependency is being resolved.

### I see multiple versions of the same dependency in the dependency graph, but I'm only declaring a single version in my build. Why is the action reporting dependency versions I'm not using?
This is almost certainly because the dependency in question is actually being resolved with different versions in different dependency configurations. 
For example, you may have one version brought in as a plugin dependency (resolved in the `classpath` configuration) and another used directly as a code dependency (resolved in the `compileClasspath` configuration).
[See here for a HOW-TO](dependency-submission.md#resolving-a-dependency-vulnerability) on getting the bottom of why the dependency is being resolved. 
By far the easiest way is to publish a Build Scan® for the workflow run: [this is easily achieved with some additional action configuration](dependency-submission.md#publishing-a-develocity-build-scan-from-your-dependency-submission-workflow).

### I'm not seeing any security vulnerabilities for any of my dependencies. How can I be sure this is working?
First check that [Dependabot Alerts](https://docs.github.com/en/code-security/dependabot/dependabot-alerts/about-dependabot-alerts) are enabled for your repository. 
Without this, your dependency graph may be populated but you won't see which dependencies are potentially vulnerable.

### How can I use Dependabot Security Updates to generate a PR to update my vulnerable dependencies?
In most cases, the Dependabot Security Updates feature is not able to automatically generate a PR to update a dependency version. 
This can be due to the vulnerable dependency being transitive, or because the Dependabot implementation doesn't understand how to update the dependency version.
In a few select cases the Dependabot security update will work and successfully generate a pull-request. For example when a direct dependency version is listed in a TOML dependency catalog.

### I'm getting many false positive Dependabot Alerts for dependencies that aren't used by my project. Why are these dependencies being reported?
The `dependency-submission` action resolves all of the dependencies in your build. This includes plugins, dependencies you've declared, test dependencies, and all transitive dependencies of these. 
It doesn't matter how the dependencies are declared: the ones being resolved by Gradle are the ones being reported.

Many people are surprised to see what dependencies are actually being resolved when they run their builds, but I'm yet to see a case where the dependencies being reported are actually incorrect. 

Please [follow the instructions here](dependency-submission.md#finding-the-source-of-a-dependency-vulnerability) to identify the source of the dependency version that is being reported.

Once you have worked out why it is being resolved, you can either [update the dependency version](dependency-submission.md#updating-the-dependency-version) 
or [exclude it from the submitted dependency graph](dependency-submission.md#limiting-the-dependencies-that-appear-in-the-dependency-graph).

