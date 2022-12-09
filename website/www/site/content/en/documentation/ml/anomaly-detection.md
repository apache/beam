---
title: "Anomaly Detection"
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

# Anomaly Detection Example

The anomaly detection example demonstrates how to set up an anomaly detection pipeline that reads text from Pub/Sub in real time, and then detects anomalies using a trained HDBSCAN clustering model.


## Dataset for Anomaly Detection
This example uses a dataset called [emotion](https://huggingface.co/datasets/emotion) that contains 20,000 English Twitter messages with 6 basic emotions: anger, fear, joy, love, sadness, and surprise. The dataset has three splits: train (for training), validation, and test (for performance evaluation). Because it contains the text and the category (class) of the dataset, it is a supervised dataset. You can use the [Hugging Face datasets page](https://huggingface.co/docs/datasets/index) to access this dataset.

The following text shows examples from the train split of the dataset:


| Text        | Type of emotion |
| :---        |    :----:   |
| im grabbing a minute to post i feel greedy wrong      | Anger       |
| i am ever feeling nostalgic about the fireplace i will know that it is still on the property   | Love        |
| ive been taking or milligrams or times recommended amount and ive fallen asleep a lot faster but i also feel like so funny | Fear |
| on a boat trip to denmark | Joy |
| i feel you know basically like a fake in the realm of science fiction | Sadness |
| i began having them several times a week feeling tortured by the hallucinations moving people and figures sounds and vibrations | Fear |

## Anomaly Detection Algorithm
[HDBSCAN](https://hdbscan.readthedocs.io/en/latest/how_hdbscan_works.html) is a clustering algorithm that extends DBSCAN by converting it into a hierarchical clustering algorithm and then extracting a flat clustering based in the stability of clusters. When trained, the model predicts `-1` if a new data point is an outlier, otherwise it predicts one of the existing clusters.


## Ingestion to Pub/Sub
Ingest the data into [Pub/Sub](https://cloud.google.com/pubsub/docs/overview) so that while clustering, the model can read the tweets from Pub/Sub. Pub/Sub is a messaging service for exchanging event data among applications and services. Streaming analytics and data integration pipelines use Pub/Sub to ingest and distribute data.

You can see the full example code for ingesting data into Pub/Sub in [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/anomaly_detection/write_data_to_pubsub_pipeline/)

The file structure for the ingestion pipeline is shown in the following diagram:

    write_data_to_pubsub_pipeline/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── utils.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

`pipeline/utils.py` contains the code for loading the emotion dataset and two `beam.DoFn` that are used for data transformation.

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline.

`config.py` defines variables that are used multiple times, like Google Cloud PROJECT_ID and NUM_WORKERS.

`setup.py` defines the packages and requirements for the pipeline to run.

`main.py` contains the pipeline code and additional functions used for running the pipeline.

### Run the Pipeline
To run the pipeline, install the required packages.For this example, you need access to a Google Cloud project, and you need to configure the Google Cloud variables, like `PROJECT_ID`, `REGION`, `PubSub TOPIC_ID`, and others in the `config.py` file.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`

The `write_data_to_pubsub_pipeline` contains four different transforms:
1. Load the emotion dataset using Hugging Face datasets (for simplicity, we take samples from three classes instead of six).
2. Associate each piece of text with a unique identifier (UID).
3. Convert the text into the format that Pub/Sub expects.
4. Write the formatted message to Pub/Sub.


## Anomaly Detection on Streaming Data

After ingesting the data to Pub/Sub, run the anomaly detection pipeline. This pipeline reads the streaming message from Pub/Sub, converts the text to an embedding using a language model, and feeds the embedding to an already trained clustering model to predict whether the message is an anomaly. One prerequisite for this pipeline is to have an HDBSCAN clustering model trained on the training split of the dataset.

You can find the full example code for anomaly detection in [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/)

The following diagram shows the file structure for the anomaly_detection pipeline:

    anomaly_detection_pipeline/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── transformations.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

`pipeline/transformations.py` contains the code for different `beam.DoFn` and additional functions that are used in pipeline.

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline.

`config.py` defines variables that are used multiple times, like the Google Cloud PROJECT_ID and NUM_WORKERS.

`setup.py` defines the packages and requirements for the pipeline to run.

`main.py` contains the pipeline code and additional functions used to run the pipeline.

### Run the Pipeline
Install the required packages and push the data to Pub/Sub.  For this example, you need access to a Google Cloud project, and you need to configure the Google Cloud variables, like `PROJECT_ID`, `REGION`, `PubSub SUBSCRIPTION_ID`, and others in the `config.py` file.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`

The pipeline includes the following steps:

1. Read the message from Pub/Sub.
2. Convert the Pub/Sub message into a `PCollection` of dictionaries where the key is the UID and the value is the Twitter text.
3. Encode the text into transformer-readable token ID integers using a tokenizer.
4. Use RunInference to get the vector embedding from a transformer-based language model.
5. Normalize the embedding.
6. Use RunInference to get anomaly prediction from a trained HDBSCAN clustering model.
7. Write the prediction to BigQuery so that the clustering model can be retrained when needed.
8. Send an email alert if an anomaly is detected.


The following code snippet shows the first two steps of the pipeline:

{{< highlight >}}
    docs = (
        pipeline
        | "Read from PubSub"
        >> ReadFromPubSub(subscription=cfg.SUBSCRIPTION_ID, with_attributes=True)
        | "Decode PubSubMessage" >> beam.ParDo(Decode())
    )
{{< /highlight >}}

The next section describes the following pipeline steps:

- Tokenizing the text
- Getting embedding using RunInference
- Getting predictions from the HDBSCAN model

### Get Embedding from a Language Model

In order to do clustering with text data, first map the text into vectors of numerical values suitable for statistical analysis. This example uses a transformer-based language model called [sentence-transformers/stsb-distilbert-base/stsb-distilbert-base](https://huggingface.co/sentence-transformers/stsb-distilbert-base). This model maps sentences and paragraphs to a 768 dimensional dense vector space, and you can use it for tasks like clustering or semantic search.

Because the language model is expecting a tokenized input instead of raw text, start by tokenizing the text. Tokenization is a preprocessing task that transforms text so that it can be fed into the model for getting predictions.

{{< highlight >}}
    normalized_embedding = (
        docs
        | "Tokenize Text" >> beam.Map(tokenize_sentence)
{{< /highlight >}}

Here, `tokenize_sentence` is a function that takes a dictionary with a text and an ID, tokenizes the text, and returns a tuple of the text and ID as well as the tokenized output.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" tokenization >}}
{{< /highlight >}}

Tokenized output is then passed to the language model to get the embeddings. To get embeddings from the language model, we use `RunInference()` from Apache Beam.

{{< highlight >}}
    | "Get Embedding" >> RunInference(KeyedModelHandler(embedding_model_handler))

{{< /highlight >}}
where `embedding_model_handler` is:

{{< highlight >}}
    embedding_model_handler = PytorchNoBatchModelHandler(
        state_dict_path=cfg.MODEL_STATE_DICT_PATH,
        model_class=ModelWrapper,
        model_params={"config": AutoConfig.from_pretrained(cfg.MODEL_CONFIG_PATH)},
        device="cpu",
    )
{{< /highlight >}}

We define `PytorchNoBatchModelHandler` as a wrapper to `PytorchModelHandler` to limit batch size to one.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/main.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/main.py" PytorchNoBatchModelHandler >}}
{{< /highlight >}}

Because the `forward()` for `DistilBertModel` doesn't return the embeddings, we custom define the model_class `ModelWrapper` to get the vector embedding.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" DistilBertModelWrapper >}}
{{< /highlight >}}

After getting the embedding for each piece of Twitter text, the embeddings are normalized, because the trained model is expecting normalized embeddings.

{{< highlight >}}
    | "Normalize Embedding" >> beam.ParDo(NormalizeEmbedding())

{{< /highlight >}}


### Get Predictions
The normalized embeddings are then forwarded to the trained HDBSCAN model to get the predictions.

{{< highlight >}}
    predictions = (
        normalized_embedding
        | "Get Prediction from Clustering Model"
        >> RunInference(model_handler=clustering_model_handler)
    )
{{< /highlight >}}

where `clustering_model_handler` is:

{{< highlight >}}
    clustering_model_handler = KeyedModelHandler(
        CustomSklearnModelHandlerNumpy(
            model_uri=cfg.CLUSTERING_MODEL_PATH, model_file_type=ModelFileType.JOBLIB
        )
    )
{{< /highlight >}}

We define `CustomSklearnModelHandlerNumpy` as a wrapper to `SklearnModelHandlerNumpy` to limit batch size to one and to override `run_inference` so that `hdbscan.approximate_predict()` is used to get anomaly predictions.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" CustomSklearnModelHandlerNumpy >}}
{{< /highlight >}}

After getting the model predictions, decode the output from `RunInference` into a dictionary. Next, store the prediction in a BigQuery table for analysis, update the HDBSCAN model, and send an email alert if the prediction is an anomaly.

{{< highlight >}}
    _ = (
        predictions
        | "Decode Prediction" >> beam.ParDo(DecodePrediction())
        | "Write to BQ" >> beam.io.WriteToBigQuery(
            table=cfg.TABLE_URI,
            schema=cfg.TABLE_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        ))

    _ = predictions | "Alert by Email" >> beam.ParDo(TriggerEmailAlert())
{{< /highlight >}}