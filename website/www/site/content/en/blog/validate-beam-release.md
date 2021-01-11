---
title:  "How to validate a Beam Release"
date:   2020-01-08 00:00:01 -0800
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

```xml
<properties>
    ...
    <beam.version>2.26.0</beam.version>
    ...
</properties>
<dependencies>
    <dependency>
        <groupId>org.apache.beam</groupId>
        <artifactId>beam-sdks-java-core</artifactId>
        <version>${beam.version}</version>
    </dependency>
    ...
</dependencies>
```

Second, you can add a new profile to your `pom.xml` file. In this new profile,
add a new repository with the staging repository for the new Beam release. For
Beam 2.27.0, this was `https://repository.apache.org/content/repositories/orgapachebeam-1149/`.

```xml
        <profile>
            <id>validaterelease</id>
            <repositories>
                <repository>
                    <id>apache.beam.newrelease</id>
                    <url>https://repository.apache.org/content/repositories/orgapachebeam-XXXX/</url>
                </repository>
            </repositories>
        </profile>
```

Once you have a `beam.version` property in your `pom.xml`, and a new profile
with the new release, you can run your `mvn` command activating the new profile,
and the new Beam version:

```
mvn test -Pvalidaterelease -Dbeam.version=2.27.0
```

This should build your project against the new release, and run basic tests.
It will allow you to run basic validations against the new Beam release.
If you find any issues, then you can share them *before* the release is
finalized, so your concerns can be addressed by the community.


### Configuring a Python build to validate a Beam release candidate

TODO
