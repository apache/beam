---
title:  "Managing Beam dependencies in Java"
date:   2023-06-23 9:00:00 -0700
categories:
  - blog
authors:
  - bvolpato

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

Managing your Java dependencies can be challenging, and if not done correctly,
it may cause a variety of problems, as incompatibilities may arise when using
specific and previously untested combinations.

To make that process easier, Beam now
provides [Bill of Materials (BOM)](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#bill-of-materials-bom-poms)
artifacts that will help dependency management tools to select compatible
combinations.

We hope this will make it easier for you to use Apache Beam, and have a simpler
transition when upgrading to newer versions.

<!--more-->

When bringing incompatible classes and libraries, the code is susceptible to
errors such
as `NoClassDefFoundError`, `NoSuchMethodError`, `NoSuchFieldError`, `FATAL ERROR in native method`.

When importing Apache Beam, the recommended way is to use Bill of Materials
(BOMs). The way BOMs work is by providing hints to the dependency management
resolution tool, so when a project imports unspecified or ambiguous dependencies,
it will know what version to use.

There are currently two BOMs provided by Beam:

- `beam-sdks-java-bom`, which manages what dependencies of Beam will be used, so
  you can specify the version only once.
- `beam-sdks-java-io-google-cloud-platform-bom`, a more comprehensive list,
  which manages Beam, along with GCP client and third-party dependencies.

Since errors are more likely to arise when using third-party dependencies,
that's the one that is recommended to use to minimize any conflicts.

In order to use BOM, the artifact has to be imported to your Maven or Gradle
dependency configurations. For example, to
use `beam-sdks-java-io-google-cloud-platform-bom`,
the following changes have to be done (and make sure that _BEAM_VERSION_ is
replaced by a valid version):

**Maven**

```xml
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.apache.beam</groupId>
      <artifactId>beam-sdks-java-google-cloud-platform-bom</artifactId>
      <version>BEAM_VERSION</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
```

**Gradle**

```
dependencies {
    implementation(platform("org.apache.beam:beam-sdks-java-google-cloud-platform-bom:BEAM_VERSION"))
}
```

After importing the BOM, specific version pinning of dependencies, for example,
anything for `org.apache.beam`, `io.grpc`, `com.google.cloud` (
including `libraries-bom`) may be removed.

Do not entirely remove the dependencies, as they are not automatically imported
by the BOM. It is important to keep the dependency without specifying a version.
For example, in Maven:

```xml
<dependency>
  <groupId>org.apache.beam</groupId>
  <artifactId>beam-sdks-java-core</artifactId>
</dependency>
```

Or Gradle:

```
implementation("org.apache.beam:beam-sdks-java-core")
```

For a full list of dependency versions that are managed by a specific BOM, the
Maven tool `help:effective-pom` can be used. For example:

```shell
mvn help:effective-pom -f ~/.m2/repository/org/apache/beam/beam-sdks-java-google-cloud-platform-bom/BEAM_VERSION/beam-sdks-java-google-cloud-platform-bom-BEAM_VERSION.pom
```

The third-party
website [mvnrepository.com](https://mvnrepository.com/artifact/org.apache.beam/beam-sdks-java-google-cloud-platform-bom/)
can also be used to display such version information.

We hope you find this
useful. [Feedback](https://beam.apache.org/community/contact-us/) and
contributions are always welcome! So feel free to create a GitHub issue, or open
a Pull Request if you encounter any problem when using those artifacts.
