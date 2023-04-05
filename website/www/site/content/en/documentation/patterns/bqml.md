---
title: "BigQuery ML integration"
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

# BigQuery ML integration

With the samples on this page we will demonstrate how to integrate models exported from [BigQuery ML (BQML)](https://cloud.google.com/bigquery-ml/docs) into your Apache Beam pipeline using [TFX Basic Shared Libraries (tfx_bsl)](https://github.com/tensorflow/tfx-bsl).

Roughly, the sections below will go through the following steps in more detail:

1. Create and train your BigQuery ML model
1. Export your BigQuery ML model
1. Create a transform that uses the brand-new BigQuery ML model

{{< language-switcher java py >}}

## Create and train your BigQuery ML model

To be able to incorporate your BQML model into an Apache Beam pipeline using tfx_bsl, it has to be in the [TensorFlow SavedModel](https://www.tensorflow.org/guide/saved_model) format. An overview that maps different model types to their export model format for BQML can be found [here](https://cloud.google.com/bigquery-ml/docs/exporting-models#export_model_formats_and_samples).

For the sake of simplicity, we'll be training a (simplified version of the) logistic regression model in the [BQML quickstart guide](https://cloud.google.com/bigquery-ml/docs/bigqueryml-web-ui-start), using the publicly available Google Analytics sample dataset (which is a [date-sharded table](https://cloud.google.com/bigquery/docs/partitioned-tables#dt_partition_shard) - alternatively, you might encounter [partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables)). An overview of all models you can create using BQML can be found [here](https://cloud.google.com/bigquery-ml/docs/introduction#supported_models_in).

After creating a BigQuery dataset, you continue to create the model, which is fully defined in SQL:

```
CREATE MODEL IF NOT EXISTS `bqml_tutorial.sample_model`
OPTIONS(model_type='logistic_reg', input_label_cols=["label"]) AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(geoNetwork.country, "") AS country
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170630'
```

The model will predict if a purchase will be made given the country of the visitor on data gathered between 2016-08-01 and 2017-06-30.

## Export your BigQuery ML model

In order to incorporate your model in an Apache Beam pipeline, you will need to export it. Prerequisites to do so are [installing the `bq` command-line tool](https://cloud.google.com/bigquery/docs/bq-command-line-tool) and [creating a Google Cloud Storage bucket](https://cloud.google.com/storage/docs/creating-buckets) to store your exported model.

Export the model using the following command:

```
bq extract -m bqml_tutorial.sample_model gs://some/gcs/path
```

## Create an Apache Beam transform that uses your BigQuery ML model

In this section we will construct an Apache Beam pipeline that will use the BigQuery ML model we just created and exported. The model can be served using Google Cloud AI Platform Prediction - for this please refer to the [AI Platform patterns](/documentation/patterns/ai-platform/). In this case, we'll be illustrating how to use the tfx_bsl library to do local predictions (on your Apache Beam workers).

First, the model needs to be downloaded to a local directory where you will be developing the rest of your pipeline (e.g. to `serving_dir/sample_model/1`).

Then, you can start developing your pipeline like you would normally do. We will be using the `RunInference` PTransform from the [tfx_bsl](https://github.com/tensorflow/tfx-bsl) library, and we will point it to our local directory where the model is stored (see the `model_path` variable in the code example). The transform takes elements of the type `tf.train.Example` as inputs and outputs elements of the type [`tensorflow_serving.apis.prediction_log_pb2.PredictionLog`](https://github.com/tensorflow/serving/blob/master/tensorflow_serving/apis/prediction_log.proto). Depending on the signature of your model, you can extract values from the output; in our case we extract `label_probs`, `label_values` and the `predicted_label` as per the [docs on the logistic regression model](https://cloud.google.com/bigquery-ml/docs/exporting-models#logistic_reg) in the `extract_prediction` function.

{{< highlight py >}}
import apache_beam
import tensorflow as tf
from google.protobuf import text_format
from tensorflow.python.framework import tensor_util
from tfx_bsl.beam import run_inference
from tfx_bsl.public.beam import RunInference
from tfx_bsl.public.proto import model_spec_pb2


inputs = tf.train.Example(features=tf.train.Features(
            feature={
                'os': tf.train.Feature(bytes_list=tf.train.BytesList(b"Microsoft"))
            })
          )

model_path = "serving_dir/sample_model/1"

def extract_prediction(response):
  yield response.predict_log.response.outputs['label_values'].string_val,
        tensor_util.MakeNdarray(response.predict_log.response.outputs['label_probs']),
        response.predict_log.response.outputs['predicted_label'].string_val

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
{{< /highlight >}}

{{< highlight java >}}
Implemented in Python.
{{< /highlight >}}
