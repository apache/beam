---
layout: section
title: "Beam Release Guide: 08 - Finalize"
section_menu: section-menu/contribute.html
permalink: /contribute/release-guide/finalize/
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

# Finalize the release

*****

Once the release candidate has been reviewed and approved by the community, the release should be finalized.
This involves the final deployment of the release candidate to the release repositories, merging of the website changes, etc.

## Deploy artifacts to Maven Central Repository

Use the Apache Nexus repository to release the staged binary artifacts to the Maven Central repository.
In the `Staging Repositories` section, find the relevant release candidate `orgapachebeam-XXX` entry and click `Release`.
Drop all other release candidates that are not being released.

The artifacts will disappear from Nexus immediately but will not appear in Maven Central for about an hour. Take a break.


*****

## Deploy Python artifacts to PyPI

1. Download everything from https://dist.apache.org/repos/dist/dev/beam/2.14.0/python/ ;
2. Keep only things that you see in https://pypi.org/project/apache-beam/#files , e.g. `.zip`, `.whl`,
   delete the `.asc`, `.sha512`;
3. Upload the new release `twine upload .` from the directory with the `.zip` and `.whl` files;


*****

## Deploy source release to `dist.apache.org`

Copy the source release from the `dev` repository to the `release` repository at `dist.apache.org` using Subversion.
Move last release artifacts from `dist.apache.org` to `archive.apache.org` using Subversion. 

__NOTE__: Only PMC members have permissions to do it, ping [dev@](mailto:dev@beam.apache.org) for assitance;

Make sure the download address for last release version is upldaed, [example PR](https://github.com/apache/beam-site/pull/478).


*****

## Git tag

Create and push a new **signed** tag for the released version by copying the tag for the final release candidate, as follows:

```
VERSION_TAG="v${RELEASE}"
git tag -s "$VERSION_TAG" "$RC_TAG"
git push github "$VERSION_TAG"
```


*****

## Merge website pull requests

* Merge the website pull request to [list the release]({{ site.baseurl }}/get-started/downloads/);
* Merge the [Java API reference manual](https://beam.apache.org/releases/javadoc/) created earlier, does not require LGTMs;
* Publish the [Python API reference manual](https://beam.apache.org/releases/pydoc/);



*****

## Mark the version as released in JIRA

In JIRA, inside [version management](https://issues.apache.org/jira/plugins/servlet/project-config/BEAM/versions),
hover over the current release and a settings menu will appear. Click `Release`, and select today’s date.

__NOTE__: Only PMC members have permissions to do it, ping [dev@](mailto:dev@beam.apache.org) for assitance;


*****

## Record keeping with ASF

Use https://reporter.apache.org to seed the information about the release into future project reports.

__NOTE__: Only PMC members have permissions to do it, ping [dev@](mailto:dev@beam.apache.org) for assitance;


*****

## Checklist to proceed to the next step

* Maven artifacts released and indexed in the [Maven Central Repository](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.beam%22);
* Source distribution available in the release repository of [dist.apache.org](https://dist.apache.org/repos/dist/release/beam/);
* Source distribution removed from the dev repository of [dist.apache.org](https://dist.apache.org/repos/dist/dev/beam/);
* Website pull request to [list the release]({{ site.baseurl }}/get-started/downloads/) and publish the [API reference manual](https://beam.apache.org/releases/javadoc/) merged;
* Release tagged in the source code repository;
* Release version finalized in JIRA;
* Release version is listed at reporter.apache.org;

**********
<a class="button button--primary" href="{{'/contribute/release-guide/post-release/'|prepend:site.baseurl}}">Next Step: Promote the Release</a>
