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

The AnomalyDetection example demonstrates how to setup an anomaly detection pipeline that reads text from PubSub in real-time, and then detects anomaly using a trained HDBSCAN clustering model.


### Dataset for Anomaly Detection
For the example, we use a dataset called [emotion](https://huggingface.co/datasets/emotion). It comprises of 20,000 English Twitter messages with 6 basic emotions: anger, fear, joy, love, sadness, and surprise. The dataset has three splits: train (for training), validation and test (for performance evaluation). It is a supervised dataset as it contains the text and the category (class) of the dataset. This dataset can easily be accessed using [HuggingFace Datasets](https://huggingface.co/docs/datasets/index).

To have a better understanding of the dataset, here are some examples from the train split of the dataset:


| Text        | Type of emotion |
| :---        |    :----:   |
| im grabbing a minute to post i feel greedy wrong      | Anger       |
| i am ever feeling nostalgic about the fireplace i will know that it is still on the property   | Love        |
| ive been taking or milligrams or times recommended amount and ive fallen asleep a lot faster but i also feel like so funny | Fear |
| on a boat trip to denmark | Joy |
| i feel you know basically like a fake in the realm of science fiction | Sadness |
| i began having them several times a week feeling tortured by the hallucinations moving people and figures sounds and vibrations | Fear |

### Anomaly Detection Algorithm
[HDBSCAN](https://hdbscan.readthedocs.io/en/latest/how_hdbscan_works.html) is a clustering algorithm which extends DBSCAN by converting it into a hierarchical clustering algorithm, and then using a technique to extract a flat clustering based in the stability of clusters. Once trained, the model will predict -1 if a new data point is an outlier, otherwise it will predict one of the existing clusters.


## Ingestion to PubSub
We first ingest the data into [PubSub](https://cloud.google.com/pubsub/docs/overview) so that while clustering we can read the tweets from PubSub. PubSub is a messaging service for exchanging event data among applications and services. It is used for streaming analytics and data integration pipelines to ingest and distribute data.

The full example code for ingesting data to PubSub can be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/anomaly_detection/write_data_to_pubsub_pipeline/)

The file structure for ingestion pipeline is:

    write_data_to_pubsub_pipeline/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── utils.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

`pipeline/utils.py` contains the code for loading the emotion dataset and two `beam.DoFn` that are used for data transformation

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline

`config.py` defines some variables like GCP PROJECT_ID, NUM_WORKERS that are used multiple times

`setup.py` defines the packages/requirements for the pipeline to run

`main.py` contains the pipeline code and some additional functions used for running the pipeline

### How to Run the Pipeline ?
First, make sure you have installed the required packages. One should have access to a Google Cloud Project and then correctly configure the GCP variables like `PROJECT_ID`, `REGION`, `PubSub TOPIC_ID` and others in `config.py`.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`


The `write_data_to_pubsub_pipeline` contains four different transforms:
1. Load emotion dataset using HuggingFace Datasets (we take samples from 3 classes instead of 6 for simplicity)
2. Associate each text with a unique identifier (UID)
3. Convert the text into a format PubSub is expecting
4. Write the formatted message to PubSub


## Anomaly Detection on Streaming Data

After having the data ingested to PubSub, we can run the anomaly detection pipeline. This pipeline reads the streaming message from PubSub, converts the text to an embedding using a language model, and feeds the embedding to an already trained clustering model to predict if the message is anomaly or not. One prerequisite for this pipeline is to have a HDBSCAN clustering model trained on the training split of the dataset.

The full example code for anomaly detection can be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/)

The file structure for anomaly_detection pipeline is:

    anomaly_detection_pipeline/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── transformations.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

`pipeline/transformations.py` contains the code for different `beam.DoFn` and additional functions that are used in pipeline

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline

`config.py` defines some variables like GCP PROJECT_ID, NUM_WORKERS that are used multiple times

`setup.py` defines the packages/requirements for the pipeline to run

`main.py` contains the pipeline code and some additional functions used for running the pipeline

### How to Run the Pipeline ?
First, make sure you have installed the required packages and you have pushed data to PubSub. One should have access to a Google Cloud Project and then correctly configure the GCP variables like `PROJECT_ID`, `REGION`, `PubSub SUBSCRIPTION_ID` and others in `config.py`.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`

The pipeline can be broken down into few simple steps:

1. Reading the message from PubSub
2. Converting the PubSub message into a PCollection of dictionaries where the key is the UID and the value is the twitter text
3. Encoding the text into transformer-readable token ID integers using a tokenizer
4. Using RunInference to get the vector embedding from a Transformer based Language Model
5. Normalizing the embedding
6. Using RunInference to get anomaly prediction from a trained HDBSCAN clustering model
7. Writing the prediction to BQ, so that clustering model can be retrained when needed
8. Sending an email alert if anomaly is detected


The code snippet for the first two steps of the pipeline:

{{< highlight >}}
    docs = (
        pipeline
        | "Read from PubSub"
        >> ReadFromPubSub(subscription=cfg.SUBSCRIPTION_ID, with_attributes=True)
        | "Decode PubSubMessage" >> beam.ParDo(Decode())
    )
{{< /highlight >}}

We will now focus on important steps of pipeline: tokenizing the text, getting embedding using RunInference and finally getting prediction from HDBSCAN model.

### Getting Embedding from a Language Model

In order to do clustering with text data, we first need to map the text into vectors of numerical values suitable for statistical analysis. We use a transformer based language model called [sentence-transformers/stsb-distilbert-base/stsb-distilbert-base](https://huggingface.co/sentence-transformers/stsb-distilbert-base). It maps sentences & paragraphs to a 768 dimensional dense vector space and can be used for tasks like clustering or semantic search. But, we first need to tokenize the text as the language model is expecting a tokenized input instead of raw text.

Tokenization is a preprocessing task that transforms text so that it can be fed into the model for getting predictions.

{{< highlight >}}
    normalized_embedding = (
        docs
        | "Tokenize Text" >> beam.Map(tokenize_sentence)
{{< /highlight >}}

Here, `tokenize_sentence` is a function that takes a dictionary with a text and an id, tokenizes the text, and returns a tuple of the text and id and the tokenized output.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" tokenization >}}
{{< /highlight >}}

Tokenized output is then passed to the language model for getting the embeddings. For getting embeddings from language model, we use `RunInference()` from beam.

{{< highlight >}}
    | "Get Embedding" >> RunInference(KeyedModelHandler(embedding_model_handler))

{{< /highlight >}}
where `embedding_model_handler` is,

{{< highlight >}}
    embedding_model_handler = PytorchNoBatchModelHandler(
        state_dict_path=cfg.MODEL_STATE_DICT_PATH,
        model_class=ModelWrapper,
        model_params={"config": AutoConfig.from_pretrained(cfg.MODEL_CONFIG_PATH)},
        device="cpu",
    )
{{< /highlight >}}

We defined `PytorchNoBatchModelHandler` as a wrapper to `PytorchModelHandler` to limit batch size to 1.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/main.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/main.py" PytorchNoBatchModelHandler >}}
{{< /highlight >}}

We custom defined the model_class `ModelWrapper` to get the vector embedding as the `forward()` for `DistilBertModel` doesn't return the embeddings.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" DistilBertModelWrapper >}}
{{< /highlight >}}

After getting the embedding for each twitter text, the embeddings are normalized as the trained model is expecting normalized embeddings.

{{< highlight >}}
    | "Normalize Embedding" >> beam.ParDo(NormalizeEmbedding())

{{< /highlight >}}


### Getting Prediction
The normalized embeddings are then forwarded to the trained HDBSCAN model for getting the predictions.

{{< highlight >}}
    predictions = (
        normalized_embedding
        | "Get Prediction from Clustering Model"
        >> RunInference(model_handler=clustering_model_handler)
    )
{{< /highlight >}}

where `clustering_model_handler` is

{{< highlight >}}
    clustering_model_handler = KeyedModelHandler(
        CustomSklearnModelHandlerNumpy(
            model_uri=cfg.CLUSTERING_MODEL_PATH, model_file_type=ModelFileType.JOBLIB
        )
    )
{{< /highlight >}}

We defined `CustomSklearnModelHandlerNumpy` as a wrapper to `SklearnModelHandlerNumpy` to limit batch size to 1 and to override the `run_inference` so that `hdbscan.approximate_predict()` is used for getting anomaly predictions.

{{< highlight file="sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/anomaly_detection/anomaly_detection_pipeline/pipeline/transformations.py" CustomSklearnModelHandlerNumpy >}}
{{< /highlight >}}

After getting the model predictions, we first the decode the output from `RunInference` into a dictionary. Afterwards, we take two different actions: i) store the prediction in a BigQuery table for analysis and updating HDBSCAN model and ii) send email alert if prediction is an anomaly.

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