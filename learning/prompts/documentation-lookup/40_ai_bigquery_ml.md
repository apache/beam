Prompt:
How can I integrate BigQuery ML with Apache Beam?

Response:
[BigQuery ML](https://cloud.google.com/bigquery/docs/bqml-introduction) allows you to develop and execute ML models using [GoogleSQL](https://cloud.google.com/bigquery/docs/introduction-sql) queries. You can seamlessly integrate these models into your Apache Beam pipeline using [TFX Basic Shared Libraries (tfx_bsl)](https://github.com/tensorflow/tfx-bsl) and perform inference on them using the RunInference API.

For an example of training a basic BigQuery ML model, refer to the [BigQuery documentation](https://cloud.google.com/bigquery/docs/bqml-introduction).

Once you have trained your model, you will need to export it. Here is an example BigQuery command to export a model to a [Google Cloud Storage](https://cloud.google.com/storage/docs/creating-buckets) bucket:

```
bq extract -m <model name> gs://<cloud storage path>
```

To incorporate your BigQuery ML model into an Apache Beam pipeline using `tfx_bsl`, it must be saved in the [TensorFlow SavedModel](https://www.tensorflow.org/guide/saved_model) format. You will need to download the model to your local directory to perform local predictions:

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

This example uses the [`RunInference`](https://beam.apache.org/documentation/transforms/python/elementwise/runinference/) transform from the `tfx_bsl` library, directing it to the local directory where the model is stored. The transform takes `tf.train.Example` type elements as inputs and produces `tensorflow_serving.apis.prediction_log_pb2.PredictionLog` type elements as outputs.

Depending on the signature of your model, you can extract values from the output. For the prediction output format of exported models for each model type, refer to the [Prediction output format](https://cloud.google.com/bigquery/docs/exporting-models#prediction_output_format) section in the BigQuery documentation.




