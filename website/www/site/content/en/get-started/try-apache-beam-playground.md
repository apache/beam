---
title: "Try Apache Beam Playground"
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

# Try Apache Beam Playground

You can try an Apache Beam examples using
[Apache Beam Playground](https://frontend-beta-dot-apache-beam-testing.appspot.com/).

## About

Beam Playground is an interactive environment to try out Beam transforms and examples.
The vision for the Playground is to be a web application where users can try out
Beam without having to install/initialize a Beam environment.

## Beam Playground WordCount Example

{{< playground "SDK_JAVA/MinimalWordCount" >}}

## How To Add New Examples

To tag any Apache Beam example/test/kata for adding to the Playground examples catalog,
it is needed to add a beam-playground tag into the file.
beam-playground tag is a yaml format comment:

{{< highlight java >}}
// beam-playground:
//   name: Name of example/Kata/Unit Test
//   description: Description for example/Kata/Unit Test
//   multifile: false
//   pipeline_options: --option1 value1 --option2 value2
//   categories:
//     - category 1
//     - category 2
//     - category N
{{< /highlight >}}
{{< highlight py >}}
# beam-playground:
#   name: Name of example/Kata/Unit Test
#   description: Description for example/Kata/Unit Test
#   multifile: false
#   pipeline_options: --option1 value1 --option2 value2
#   categories:
#     - category 1
#     - category 2
#     - category N
{{< /highlight >}}
{{< highlight go >}}
// beam-playground:
//   name: Name of example/Kata/Unit Test
//   description: Description for example/Kata/Unit Test
//   multifile: false
//   pipeline_options: --option1 value1 --option2 value2
//   categories:
//     - category 1
//     - category 2
//     - category N
{{< /highlight >}}

The beam playground tag consists of the following **required** elements:

- `beam-playground` - tag title.
- `name` - string field. Name of the Beam example/test/kata.
- `description` - string field. Description of the beam example/test/kata.
- `multifile` - boolean field. Specifies if the given example consists of multiple files or not.
- `pipeline_options` - string field. Contains information about pipeline options of the beam example/test/kata.
- `categories` - list type field.
Lists categories this example is included into. Available categories are listed in playground/categories.yaml.
If some Beam example/kata/test contains a category that is not included in playground/categories.yaml
then the developer can submit PR to add this category into the file.

More details on examples in Apache Beam Playground can be found
[here](https://docs.google.com/document/d/1LBeGVTYwJHYbtmLt06OjhBR1CJ1Wgz18MEZjvNkuofc/edit?usp=sharing).

## Next Steps

* Walk through examples in [Apache Beam Playground](https://frontend-beta-dot-apache-beam-testing.appspot.com/).
* Leave your feedback in [Apache Beam Playground](https://frontend-beta-dot-apache-beam-testing.appspot.com/).
* Join the Beam [users@](/community/contact-us) mailing list.
* If you're interested in contributing to the Apache Beam Playground codebase, see the [Contribution Guide](/contribute).

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
