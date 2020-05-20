---
title: "AI Platform integration patterns"
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

# AI Platform integration patterns

This page describes common patterns in pipelines with Google Cloud AI Platform transforms.

{{< language-switcher java py >}}

## Analysing the structure and meaning of text

This section shows how to use [Google Cloud Natural Language API](https://cloud.google.com/natural-language) to perform text analysis.

Beam provides a PTransform called [AnnotateText](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.gcp.naturallanguageml.html#apache_beam.ml.gcp.naturallanguageml.AnnotateText). The transform takes a PCollection of type [Document](https://beam.apache.org/releases/pydoc/current/apache_beam.ml.gcp.naturallanguageml.html#apache_beam.ml.gcp.naturallanguageml.Document). Each Document object contains various information about text. This includes the content, whether it is a plain text or HTML, an optional language hint and other settings.
`AnnotateText` produces response object of type `AnnotateTextResponse` returned from the API. `AnnotateTextResponse` is a protobuf message which contains a lot of attributes, some of which are complex structures.

Here is an example of a pipeline that creates in-memory PCollection of strings, changes each string to Document object and invokes Natural Language API. Then, for each response object, a function is called to extract certain results of analysis.

{{< highlight py >}}
{{< github_sample "/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py" nlp_analyze_text >}}
{{< /highlight >}}

{{< highlight java >}}
// TODO: Write an example for Java SDK.
{{< /highlight >}}


### Extracting sentiments

The function for extracting information about sentence-level and document-level sentiments is shown in the next code snippet.

{{< highlight py >}}
{{< github_sample "/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py" nlp_extract_sentiments >}}
{{< /highlight >}}

{{< highlight java >}}
// TODO: Write an example for Java SDK.
{{< /highlight >}}

Sentence-level sentiments can be found in `sentences` attribute. `sentences` behaves like a standard Python sequence, therefore all core language features (like iteration or slicing) will work. The snippet loops over `sentences` and, for each sentence, extracts the sentiment score. 

Overall sentiment can be found in `document_sentiment` attribute. 

The output is:

{{< highlight py >}}
{'sentences': [{'My experience so far has been fantastic!': 0.8999999761581421}, {"I'd really recommend this product.": 0.8999999761581421}], 'document_sentiment': 0.8999999761581421}
{{< /highlight >}}

{{< highlight java >}}
// TODO: Write an example for Java SDK.
{{< /highlight >}}

### Extracting entities

The next function inspects the response for entities and returns the names and the types of those entities. 

{{< highlight py >}}
{{< github_sample "/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py" nlp_extract_entities >}}
{{< /highlight >}}

{{< highlight java >}}
// TODO: Write an example for Java SDK.
{{< /highlight >}}

Entities can be found in `entities` attribute. Just like before, `entities` is a sequence, that's why list comprehension is a viable choice. The most tricky part is interpreting the types of entities. Natural Language API defines entity types as enum. In a response object, entity types are returned as integers. That's why a user has to instantiate `naturallanguageml.enums.Entity.Type` to access a human-readable name.

The output is:

{{< highlight py >}}
[{'name': 'experience', 'type': 'OTHER'}, {'name': 'product', 'type': 'CONSUMER_GOOD'}]
{{< /highlight >}}

{{< highlight java >}}
// TODO: Write an example for Java SDK.
{{< /highlight >}}

### Accessing sentence dependency tree

The following code loops over the sentences and, for each sentence, builds an adjacency list that represents a dependency tree. For more information on what dependency tree is, see [Morphology & Dependency Trees](https://cloud.google.com/natural-language/docs/morphology#dependency_trees).

{{< highlight py >}}
{{< github_sample "/apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets.py" analyze_dependency_tree >}}
{{< /highlight >}}

{{< highlight java >}}
// TODO: Write an example for Java SDK.
{{< /highlight >}}

The output is below. For better readability, indexes are replaced by text which they refer to:

```
[
  {
    "experience": [
      "My"
    ],
    "been": [
      "experience",
      "far",
      "has",
      "been",
      "fantastic",
      "!"
    ],
    "far": [
      "so"
    ]
  },
  {
    "recommend": [
      "I",
      "'d",
      "really",
      "recommend",
      "product",
      "."
    ],
    "product": [
      "this"
    ]
  }
]
```
