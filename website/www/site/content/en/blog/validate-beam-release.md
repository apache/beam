---
title:  "How to validate a Beam Release"
date:   2021-06-08 00:00:01 -0800
categories:
  - blog
authors:
  - pabloem

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

Performing new releases is a core responsibility of any software project.
It is even more important in the culture of Apache projects. Releases are
the main flow of new code / features among the community of a project.

Beam is no exception: We aspire to keep a release cadence of about 6 weeks,
and try to work with the community to release useful new features, and to
keep Beam useful.

### Configure a Java build to validate a Beam release candidate

First of all, it would be useful to have a single property in your `pom.xml`
where you keep the global Beam version that you're using. Something like this
in your `pom.xml`:

{{< highlight java >}}
&lt;properties&gt;
    ...
    &lt;beam.version&gt;2.26.0&lt;/beam.version&gt;
    ...
&lt;/properties&gt;
&lt;dependencies&gt;
    &lt;dependency&gt;
        &lt;groupId&gt;org.apache.beam&lt;/groupId&gt;
        &lt;artifactId&gt;beam-sdks-java-core&lt;/artifactId&gt;
        &lt;version&gt;${beam.version}&lt;/version&gt;
    &lt;/dependency&gt;
    ...
&lt;/dependencies&gt;
{{< /highlight >}}

Second, you can add a new profile to your `pom.xml` file. In this new profile,
add a new repository with the staging repository for the new Beam release. For
Beam 2.27.0, this was `https://repository.apache.org/content/repositories/orgapachebeam-1149/`.

{{< highlight java >}}
        &lt;profile&gt;
            &lt;id&gt;validaterelease&lt;/id&gt;
            &lt;repositories&gt;
                &lt;repository&gt;
                    &lt;id&gt;apache.beam.newrelease&lt;/id&gt;
                    &lt;url&gt;${beam.release.repo}&lt;/url&gt;
                &lt;/repository&gt;
            &lt;/repositories&gt;
        &lt;/profile&gt;
{{< /highlight >}}

Once you have a `beam.version` property in your `pom.xml`, and a new profile
with the new release, you can run your `mvn` command activating the new profile,
and the new Beam version:

```
mvn test -Pvalidaterelease \
         -Dbeam.version=2.27.0 \
         -Dbeam.release.repo=https://repository.apache.org/content/repositories/orgapachebeam-XXXX/
```

This should build your project against the new release, and run basic tests.
It will allow you to run basic validations against the new Beam release.
If you find any issues, then you can share them *before* the release is
finalized, so your concerns can be addressed by the community.


### Configuring a Python build to validate a Beam release candidate

For Python SDK releases, you can install SDK from Pypi, by enabling the
installation of pre-release artifacts.

First, make sure that your `requirements.txt` or `setup.py` files allow
for Beam versions above the current one. Something like this should install
the latest available version:

```
apache-beam<=3.0.0
```

With that, you can ask `pip` to install pre-release versions of Beam in your
environment:

```
pip install --pre apache-beam
```

With that, the Beam version in your environment will be the latest release
candidate, and you can go ahead and run your tests to verify that everything
works well.

### Validating Prism Runner RC against RC SDKs

Replace v2.59.0-RC1 with the tag of the RC version being validated.

#### Python

To validate the prism runner with Python,
`--runner=PrismRunner --prism_location=https://github.com/apache/beam/releases/tag/v2.59.0-RC1 --prism_beam_version_override=v2.59.0`

* The `runner` flag sets Beam to use Prism.
* The `prism_location` sets the source of Prism assets.
* The `prism_beam_version_override` flag sets what those artifacts are labeled as.
* The assets are packaged as the final release version, so the override is required.

#### Java

For Gradle, add the Prism, and the JAMM depdendencies to your `build.gradle`.

```
    implementation "org.apache.beam:beam-runners-prism-java:2.59.0"
    implementation "com.github.jbellis:jamm:0.4.0"
```

Then add the following flags, substituting the version accordingly.

`--runner=PrismRunner --prismLocation="https://github.com/apache/beam/releases/tag/v2.59.0-RC1/" --prismVersionOverride=v2.59.0

* The `runner` flag sets Beam to use Prism.
* The `prismLocation` sets the source of Prism assets, specifically the zip file of the version in question.


### Configuring a Go build to validate a Beam release candidate

For Go SDK releases, you can fetch the Go SDK RC using [`go get`](https://golang.org/ref/mod#go-get),
by requesting the specific pre-release version.

For example, to request the first release candidate for 2.44.0:

```
go get -d github.com/apache/beam/sdks/v2@v2.44.0-RC1
```

With that, the Beam version in your `go.mod` will be the specified release candidate.
You can go ahead and run your tests to verify that everything works well.

You may need to also specify the RC's matching container when running a job.
Use the `--environment_config` flag to specify the release candidate container:
eg. `--environment_config=apache/beam_go_sdk:2.44.0rc1`
