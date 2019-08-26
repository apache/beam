---
layout: section
title: "Beam Release Guide: 06 - Prepare Docs"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/prepare-docs/
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


# Prepare the documentation

*****


## Update and Verify Javadoc

The build with `-PisRelease` creates the combined Javadoc for the release in `sdks/java/javadoc`.

The file `sdks/java/javadoc/build.gradle` contains a list of modules to include
in and exclude, plus a list of offline URLs that populate links from Beam's
Javadoc to the Javadoc for other modules that Beam depends on.

* Confirm that new modules added since the last release have been added to the
  inclusion list as appropriate.

* Confirm that the excluded package list is up to date.

* Verify the version numbers for offline links match the versions used by Beam. If
  the version number has changed, download a new version of the corresponding
  `<module>-docs/package-list` file.


*****


## Build the Pydoc API reference

Make sure you have ```tox``` installed: 

```
pip install tox
```

Create the Python SDK documentation using sphinx by running a helper script.

```
cd sdks/python && tox -e docs
```

By default the Pydoc is generated in `sdks/python/target/docs/_build`. 

Set `${PYDOC_ROOT}` be the absolute path to `_build`.


*****


## Propose pull requests for website updates

Beam publishes API reference manuals for each release on the website. For Java
and Python SDKs, that’s Javadoc and PyDoc, respectively. The final step of
building the candidate is to propose website pull requests that update these
manuals.

Merge the pull requests only after finalizing the release. To avoid invalid
redirects for the 'current' version, merge these PRs in the order listed. Once
the PR is merged, the new contents will get picked up automatically and served
to the Beam website, usually within an hour.


### Javadoc PR against `apache/beam-site`

This pull request is against the `apache/beam-site` repo, on the `release-docs`
branch.

* Add the new Javadoc to [SDK API Reference page](https://beam.apache.org/releases/javadoc/) page, as follows:
  * Unpack the Maven artifact `org.apache.beam:beam-sdks-java-javadoc` into some temporary location. Call this `${JAVADOC_TMP}`.
  * Copy the generated Javadoc into the website repository: `cp -r ${JAVADOC_TMP} javadoc/${RELEASE}`.
* Add the new Pydoc to [SDK API Reference page](https://beam.apache.org/releases/pydoc/) page, as follows:
  * Copy the generated Pydoc into the website repository: `cp -r ${PYDOC_ROOT} pydoc/${RELEASE}`.
  * Remove `.doctrees` directory.
* Stage files using: `git add --all javadoc/ pydoc/`.



### Website update for new release version

This pull request is against the `apache/beam` repo, on the `master` branch.

* Update the `release_latest` version flag in `/website/_config.yml`, and list
  the new release in `/website/src/get-started/downloads.md`, linking to the
  source code download and the Release Notes in JIRA.
* Update the `RedirectMatch` rule in
  [/website/src/.htaccess](https://github.com/apache/beam/blob/master/website/src/.htaccess)
  to point to the new release. See file history for examples.



### Blog post

Write a blog post similar to https://beam.apache.org/blog/2019/07/31/beam-2.14.0.html

__Tip__: Use git log to find contributors to the releases. (e.g: `git log --pretty='%aN' ^v2.10.0 v2.11.0 | sort | uniq`).
Make sure to clean it up, as there may be duplicate or incorrect user names.

__NOTE__: Make sure to include any breaking changes, even to `@Experimental` features,
all major features and bug fixes, and all known issues.

Template:

```

    We are happy to present the new {$RELEASE_VERSION} release of Beam. This release includes both improvements and new functionality.
    See the [download page]({{ site.baseurl }}/get-started/downloads/{$DOWNLOAD_ANCHOR}) for this release.<!--more-->
    For more information on changes in {$RELEASE_VERSION}, check out the
    [detailed release notes]({$JIRA_RELEASE_NOTES}).
    
    ## Highlights
    
     * New highly anticipated feature X added to Python SDK ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
     * New highly anticipated feature Y added to JavaSDK ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).
    
    {$TOPICS e.g.:}
    ### I/Os
    * Support for X source added (Java) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
    {$TOPICS}
    
    ### New Features / Improvements
    
    * X feature added (Python) ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
    * Y feature added (Java) [BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y).
    
    ### Breaking Changes
    
    * X behavior was changed ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
    * Y behavior was changed ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-X)).
    
    ### Deprecations
    
    * X behavior is deprecated and will be removed in X versions ([BEAM-X](https://issues.apache.org/jira/browse/BEAM-X)).
    
    ### Bugfixes
    
    * Fixed X (Python) ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-X)).
    * Fixed Y (Java) ([BEAM-Y](https://issues.apache.org/jira/browse/BEAM-Y)).
    
    ### Known Issues
    
    * {$KNOWN_ISSUE_1}
    * {$KNOWN_ISSUE_2}
    
    
    ## List of Contributors
    
    According to git shortlog, the following people contributed to the 2.XX.0 release. Thank you to all contributors!
    
    ${CONTRIBUTORS}

```


*****

## Checklist to proceed to the next step

1. Maven artifacts deployed to the staging repository of [repository.apache.org](https://repository.apache.org/content/repositories/);
1. Source distribution deployed to the dev repository of [dist.apache.org](https://dist.apache.org/repos/dist/dev/beam/);
1. Website pull request proposed to list the
   [release]({{ site.baseurl }}/get-started/downloads/), 
   publish the [Java API reference manual](https://beam.apache.org/releases/javadoc/), 
   and publish the [Python API reference manual](https://beam.apache.org/releases/pydoc/);

You can (optionally) also do additional verification by:
1. Check that Python zip file contains the `README.md`, `NOTICE`, and `LICENSE` files;
1. Check hashes (e.g. `md5sum -c *.md5` and `sha1sum -c *.sha1`);
1. Check signatures (e.g. `gpg --verify apache-beam-1.2.3-python.zip.asc apache-beam-1.2.3-python.zip`);
1. `grep` for legal headers in each file;
1. Run all jenkins suites and include links to passing tests in the voting email. (Select "Run with parameters");

**********

<a class="button button--primary" href="{{'/contribute/release-guide/validate-candidate/'|prepend:site.baseurl}}">Next Step: Validate the Release Candidate</a>
