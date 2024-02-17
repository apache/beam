Prompt:
How can I integrate BigQuery ML with Apache Beam?

Response:
[BigQuery ML](https://cloud.google.com/bigquery/docs/bqml-introduction) lets you create and run ML models by using `GoogleSQL` queries. You can integrate those models into your Apache Beam pipeline using [TFX Basic Shared Libraries (tfx_bsl)](https://github.com/tensorflow/tfx-bsl) and run inference on them using the `RunInference` API.

For an example of training a simple `BQML` model please see [here](https://cloud.google.com/bigquery-ml/docs/bigqueryml-web-ui-start).

After you have trained your model, you need to export it. Below is an example of BigQuery command to export a model to a [Google Cloud Storage](https://cloud.google.com/storage/docs/creating-buckets) bucket:

```
bq extract -m <model name> gs://<cloud storage path>
```

In order to incorporate your `BQML` model into an Apache Beam pipeline using `tfx_bsl` it has to be saved in the [TensorFlow SavedModel](https://www.tensorflow.org/guide/saved_model) format. You have to download model to local directory to do local predictions.
```python
import apache_beam
import tensorflow as tf
from google.protobuf import text_format
from tensorflow.python.framework import tensor_util
from tfx_bsl.beam import run_inference
from tfx_bsl.public.beam import RunInference
from tfx_bsl.public.proto import model_spec_pb2


inputs = tf.train.Example(...)

model_path = <path to the directory where the model is stored>

def extract_prediction(response):
    # Extract the prediction from the response depending on the signature of the model

with beam.Pipeline() as p:
    res = (
        p
        | beam.Create([inputs])
        | RunInference(
            model_spec_pb2.InferenceSpecType(
                saved_model_spec=model_spec_pb2.SavedModelSpec(
                    model_path=model_path,
                    signature_name=['serving_default'])))
        | beam.ParDo(extract_prediction)
    )
```

This example uses the `RunInference` `PTransform` from the `tfx_bsl` library, and we  point it to your local directory where the model is stored.
The transform takes elements of the type `tf.train.Example` as inputs and outputs elements of the type `tensorflow_serving.apis.prediction_log_pb2.PredictionLog`. You can extract values from the output depending on the signature of the model. See [here](https://cloud.google.com/bigquery/docs/exporting-models#prediction_output_format) for the output format of `BQML` models.




