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
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" nlp_analyze_text >}}
{{< /highlight >}}

{{< highlight java >}}
// Java examples will be available on Beam 2.23 release.
{{< /highlight >}}


### Extracting sentiments

This is a part of response object returned from the API. Sentence-level sentiments can be found in `sentences` attribute. `sentences` behaves like a standard Python sequence, therefore all core language features (like iteration or slicing) will work. Overall sentiment can be found in `document_sentiment` attribute.

```
sentences {
  text {
    content: "My experience so far has been fantastic!"
  }
  sentiment {
    magnitude: 0.8999999761581421
    score: 0.8999999761581421
  }
}
sentences {
  text {
    content: "I\'d really recommend this product."
    begin_offset: 41
  }
  sentiment {
    magnitude: 0.8999999761581421
    score: 0.8999999761581421
  }
}

...many lines omitted

document_sentiment {
  magnitude: 1.899999976158142
  score: 0.8999999761581421
}
```

The function for extracting information about sentence-level and document-level sentiments is shown in the next code snippet.

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" nlp_extract_sentiments >}}
{{< /highlight >}}

{{< highlight java >}}
// Java examples will be available on Beam 2.23 release.
{{< /highlight >}}

The snippet loops over `sentences` and, for each sentence, extracts the sentiment score. 

The output is:

```
{"sentences": [{"My experience so far has been fantastic!": 0.8999999761581421}, {"I'd really recommend this product.": 0.8999999761581421}], "document_sentiment": 0.8999999761581421}
```

### Extracting entities

The next function inspects the response for entities and returns the names and the types of those entities. 

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" nlp_extract_entities >}}
{{< /highlight >}}

{{< highlight java >}}
// Java examples will be available on Beam 2.23 release.
{{< /highlight >}}

Entities can be found in `entities` attribute. Just like before, `entities` is a sequence, that's why list comprehension is a viable choice. The most tricky part is interpreting the types of entities. Natural Language API defines entity types as enum. In a response object, entity types are returned as integers. That's why a user has to instantiate `naturallanguageml.enums.Entity.Type` to access a human-readable name.

The output is:

```
[{"name": "experience", "type": "OTHER"}, {"name": "product", "type": "CONSUMER_GOOD"}]
```

### Accessing sentence dependency tree

The following code loops over the sentences and, for each sentence, builds an adjacency list that represents a dependency tree. For more information on what dependency tree is, see [Morphology & Dependency Trees](https://cloud.google.com/natural-language/docs/morphology#dependency_trees).

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" analyze_dependency_tree >}}
{{< /highlight >}}

{{< highlight java >}}
// Java examples will be available on Beam 2.23 release.
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

## Getting predictions

This section shows how to use [Google Cloud AI Platform Prediction](https://cloud.google.com/ai-platform/prediction/docs/overview) to make predictions about new data from a cloud-hosted machine learning model.
 
[tfx_bsl](https://github.com/tensorflow/tfx-bsl) is a library with a Beam PTransform called `RunInference`. `RunInference` is able to perform an inference that can use an external service endpoint for receiving data. When using a service endpoint, the transform takes a PCollection of type `tf.train.Example` and, for every batch of elements, sends a request to AI Platform Prediction. The size of a batch is automatically computed. For more details on how Beam finds the best batch size, refer to a docstring for [BatchElements](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html?highlight=batchelements#apache_beam.transforms.util.BatchElements). Currently, the transform does not support using `tf.train.SequenceExample` as input, but the work is in progress.    
 
 The transform produces a PCollection of type `PredictionLog`, which contains predictions.

Before getting started, deploy a TensorFlow model to AI Platform Prediction. The cloud service manages the infrastructure needed to handle prediction requests in both efficient and scalable way. Do note that only TensorFlow models are supported by the transform. For more information, see [Exporting a SavedModel for prediction](https://cloud.google.com/ai-platform/prediction/docs/exporting-savedmodel-for-prediction).

Once a machine learning model is deployed, prepare a list of instances to get predictions for. To send binary data, make sure that the name of an input ends in `_bytes`. This will base64-encode data before sending a request.

### Example
Here is an example of a pipeline that reads input instances from the file, converts JSON objects to `tf.train.Example` objects and sends data to AI Platform Prediction. The content of a file can look like this:

```
{"input": "the quick brown"}
{"input": "la bruja le"}
``` 

The example creates `tf.train.BytesList` instances, thus it expects byte-like strings as input. However, other data types, like `tf.train.FloatList` and `tf.train.Int64List`, are also supported by the transform.

Here is the code:

{{< highlight py >}}
import json

import apache_beam as beam

import tensorflow as tf
from tfx_bsl.beam.run_inference import RunInference
from tfx_bsl.proto import model_spec_pb2

def convert_json_to_tf_example(json_obj):
  samples = json.loads(json_obj)
  for name, text in samples.items():
      value = tf.train.Feature(bytes_list=tf.train.BytesList(
        value=[text.encode('utf-8')]))
      feature = {name: value}
      return tf.train.Example(features=tf.train.Features(feature=feature))

with beam.Pipeline() as p:
     _ = (p
         | beam.io.ReadFromText('gs://my-bucket/samples.json')
         | beam.Map(convert_json_to_tf_example)
         | RunInference(
             model_spec_pb2.InferenceEndpoint(
                 model_endpoint_spec=model_spec_pb2.AIPlatformPredictionModelSpec(
                     project_id='my-project-id',
                     model_name='my-model-name',
                     version_name='my-model-version'))))
{{< /highlight >}}

{{< highlight java >}}
// Getting predictions is not yet available for Java. [BEAM-9501]
{{< /highlight >}}
