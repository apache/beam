---
title: "Try Beam Playground (Beta)"
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

# Try Beam Playground (Beta)

Beam Playground is an interactive environment to try out Beam transforms and examples
without having to install Apache Beam in your environment.

You can try the available Apache Beam examples at
[Beam Playground](https://play.beam.apache.org/).

## Beam Playground WordCount Example

{{< playground height="700px" >}}
{{< playground_snippet language="java" path="SDK_JAVA_MinimalWordCount" >}}
{{< playground_snippet language="py" path="SDK_PYTHON_WordCountWithMetrics" >}}
{{< playground_snippet language="go" path="SDK_GO_MinimalWordCount" >}}
{{< playground_snippet language="scio" path="SDK_SCIO_MinimalWordCount" >}}
{{< /playground >}}

## How To Add New Examples

To add an Apache Beam example/test/kata into Beam Playground catalog,
add the `beam-playground` tag into the file to be added.
`beam-playground` tag is a yaml format comment:

{{< highlight java >}}
// beam-playground:
//   name: Name of the example/test/kata
//   description: Description of the example/test/kata
//   multifile: false
//   pipeline_options: --option1 value1 --option2 value2
//   default_example: false
//   context_line: 10
//   categories:
//     - category 1
//     - category 2
//     - category N

// example code
{{< /highlight >}}
{{< highlight py >}}
# beam-playground:
#   name: Name of the example/test/kata
#   description: Description of the example/test/kata
#   multifile: false
#   pipeline_options: --option1 value1 --option2 value2
#   default_example: false
#   context_line: 10
#   categories:
#     - category 1
#     - category 2
#     - category N

# example code
{{< /highlight >}}
{{< highlight go >}}
// beam-playground:
//   name: Name of the example/test/kata
//   description: Description of the example/test/kata
//   multifile: false
//   pipeline_options: --option1 value1 --option2 value2
//   default_example: false
//   context_line: 10
//   categories:
//     - category 1
//     - category 2
//     - category N

// example code
{{< /highlight >}}

The 'beam-playground' tag consists of the following **required** elements:

- `beam-playground` - tag title.
- `name` - string field. Name of the Beam example/test/kata that will be displayed in the Beam Playground
examples catalog.
- `description` - string field. Description of the Beam example/test/kata that will be displayed in Beam Playground.
- `multifile` - boolean field. Specifies if the given example consists of multiple files or not.
- `pipeline_options` - string field (optional). Contains information about pipeline options of the Beam example/test/kata.
- `default_example` - boolean field (optional). Specifies if the given example is default or not. If some example is tagged
  as default it means that this example is shown when its SDK is chosen in Beam Playground.
  Only one example can be set as a default for each SDK.
- `context_line` - integer field. The line where the main part of the Beam example/test/kata begins.
- `categories` - list type field.
Lists categories this example is included into. Available categories are listed in
[playground/categories.yaml](https://github.com/apache/beam/blob/master/playground/categories.yaml).
If some Beam example/kata/test needs to add a new category, then please submit PR with the changes to `categories.yaml`.
After the category has been added, it can be used in the examples.

More details on examples in Apache Beam Playground can be found
[here](https://docs.google.com/document/d/1LBeGVTYwJHYbtmLt06OjhBR1CJ1Wgz18MEZjvNkuofc/edit?usp=sharing).

## Next Steps

* Try examples in [Apache Beam Playground](https://play.beam.apache.org/).
* Submit feedback using "Enjoying Playground?" in
[Apache Beam Playground](https://play.beam.apache.org/) or via
[this form](https://docs.google.com/forms/d/e/1FAIpQLSd5_5XeOwwW2yjEVHUXmiBad8Lxk-4OtNcgG45pbyAZzd4EbA/viewform?usp=pp_url).
* Join the Beam [users@](/community/contact-us) mailing list.
* If you're interested in contributing to the Apache Beam Playground codebase, see the [Contribution Guide](/contribute).

Please don't hesitate to [reach out](/community/contact-us) if you encounter any issues!
