---
title: "Online Clustering"
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

# Online Clustering Example

The online clustering example demonstrates how to set up a real-time clustering pipeline that can read text from Pub/Sub, convert the text into an embedding using a language model, and cluster the text using BIRCH.

## Dataset for Clustering
This example uses a dataset called [emotion](https://huggingface.co/datasets/emotion) that contains 20,000 English Twitter messages with 6 basic emotions: anger, fear, joy, love, sadness, and surprise. The dataset has three splits: train, validation, and test. Because it contains the text and the category (class) of the dataset, it's a supervised dataset. To access this dataset, use the [Hugging Face datasets page](https://huggingface.co/docs/datasets/index).

The following text shows examples from the train split of the dataset:


| Text        | Type of emotion |
| :---        |    :----:   |
| im grabbing a minute to post i feel greedy wrong      | Anger       |
| i am ever feeling nostalgic about the fireplace i will know that it is still on the property   | Love        |
| ive been taking or milligrams or times recommended amount and ive fallen asleep a lot faster but i also feel like so funny | Fear |
| on a boat trip to denmark | Joy |
| i feel you know basically like a fake in the realm of science fiction | Sadness |
| i began having them several times a week feeling tortured by the hallucinations moving people and figures sounds and vibrations | Fear |

## Clustering Algorithm
For the clustering of tweets, we use an incremental clustering algorithm called BIRCH. It stands for balanced iterative reducing and clustering using hierarchies, and it is an unsupervised data mining algorithm used to perform hierarchical clustering over particularly large datasets. An advantage of BIRCH is its ability to incrementally and dynamically cluster incoming, multi-dimensional metric data points in an attempt to produce the best quality clustering for a given set of resources (memory and time constraints).


## Ingestion to Pub/Sub
The example starts by ingesting the data into [Pub/Sub](https://cloud.google.com/pubsub/docs/overview) so that we can read the tweets from Pub/Sub while clustering. Pub/Sub is a messaging service for exchanging event data among applications and services. Streaming analytics and data integration pipelines use Pub/Sub to ingest and distribute data.

You can find the full example code for ingesting data into Pub/Sub in [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/online_clustering/write_data_to_pubsub_pipeline/)

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

`config.py` defines some variables that are used multiple times, like GCP PROJECT_ID and NUM_WORKERS.

`setup.py` defines the packages and requirements for the pipeline to run.

`main.py` contains the pipeline code and some additional function used for running the pipeline.

## Run the Pipeline
First, install the required packages.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`


The `write_data_to_pubsub_pipeline` contains four different transforms:
1. Load the emotion dataset using Hugging Face datasets (for simplicity, we take samples from three classes instead of six).
2. Associate each piece of text with a unique identifier (UID).
3. Convert the text into the format that Pub/Sub expects.
4. Write the formatted message to Pub/Sub.


## Clustering on Streaming Data

After ingesting the data to Pub/Sub, examine the second pipeline, where we read the streaming message from Pub/Sub, convert the text to a embedding using a language model, and cluster the embedding using BIRCH.

You can find the full example code for all the steps mentioned previously in [GitHub](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/online_clustering/clustering_pipeline/).


The file structure for clustering_pipeline is:

    clustering_pipeline/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── transformations.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

`pipeline/transformations.py` contains the code for the different `beam.DoFn` that are used in the pipeline.

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline.

`config.py` defines variables that are used multiple times, like Google Cloud PROJECT_ID and NUM_WORKERS.

`setup.py` defines the packages and requirements for the pipeline to run.

`main.py` contains the pipeline code and some additional functions used for running the pipeline.

### Run the Pipeline
Install the required packages and push data to Pub/Sub.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`

The pipeline can be broken down into the following steps:

1. Read the message from Pub/Sub.
2. Convert the Pub/Sub message into a `PCollection` of dictionaries where the key is the UID and the value is the Twitter text.
3. Encode the text into transformer-readable token ID integers using a tokenizer.
4. Use RunInference to get the vector embedding from a transformer-based language model.
5. Normalize the embedding for clustering.
6. Perform BIRCH clustering using stateful processing.
7. Print the texts assigned to clusters.

The following code shows the first two steps of the pipeline, where a message from Pub/Sub is read and converted into a dictionary.

{{< highlight >}}
    docs = (
        pipeline
        | "Read from PubSub"
        >> ReadFromPubSub(subscription=cfg.SUBSCRIPTION_ID, with_attributes=True)
        | "Decode PubSubMessage" >> beam.ParDo(Decode())
    )
{{< /highlight >}}


The next sections examine three important pipeline steps:

1. Tokenize the text.
2. Feed the tokenized text to get embedding from a transformer-based language model.
3. Perform clustering using [stateful processing](/blog/stateful-processing/).


### Get Embedding from a Language Model

In order cluster text data, you need to map the text into vectors of numerical values suitable for statistical analysis. This example uses a transformer-based language model called [sentence-transformers/stsb-distilbert-base/stsb-distilbert-base](https://huggingface.co/sentence-transformers/stsb-distilbert-base). It maps sentences and paragraphs to a 768 dimensional dense vector space, and you can use it for tasks like clustering or semantic search.

Because the language model is expecting a tokenized input instead of raw text, start by tokenizing the text. Tokenization is a preprocessing task that transforms text so that it can be fed into the model for getting predictions.

{{< highlight >}}
    normalized_embedding = (
        docs
        | "Tokenize Text" >> beam.Map(tokenize_sentence)
{{< /highlight >}}

Here, `tokenize_sentence` is a function that takes a dictionary with a text and an ID, tokenizes the text, and returns a tuple (text, id) and the tokenized output.

Tokenized output is then passed to the language model for getting the embeddings. For getting embeddings from the language model, we use `RunInference()` from Apache Beam.

{{< highlight >}}
    | "Get Embedding" >> RunInference(KeyedModelHandler(model_handler))

{{< /highlight >}}

To make better clusters, after getting the embedding for each piece of Twitter text, the embeddings are normalized.

{{< highlight >}}
    | "Normalize Embedding" >> beam.ParDo(NormalizeEmbedding())

{{< /highlight >}}


### StatefulOnlineClustering
Because the data is streaming, you need to use an iterative clustering algorithm, like BIRCH. And because the algorithm is iterative, you need a mechanism to store the previous state so that when Twitter text arrives, it can be updated. **Stateful processing** enables a `DoFn` to have persistent state, which can be read and written during the processing of each element. For more information about stateful processing, see [Stateful processing with Apache Beam](/blog/stateful-processing/).

In this example, every time a new message is read from Pub/Sub, you retrieve the existing state of the clustering model, update it, and write it back to the state.

{{< highlight >}}
    clustering = (
        normalized_embedding
        | "Map doc to key" >> beam.Map(lambda x: (1, x))
        | "StatefulClustering using Birch" >> beam.ParDo(StatefulOnlineClustering())
    )
{{< /highlight >}}

Because BIRCH doesn't support parallelization, you need to make sure that only one worker is doing all of the stateful processing. To do that, use `Beam.Map` to associate each text to the same key `1`.

`StatefulOnlineClustering` is a `DoFn` that takes an embedding of a text and updates the clustering model. To store the state, it uses the `ReadModifyWriteStateSpec` state object, which acts as a container for storage.

{{< highlight >}}

class StatefulOnlineClustering(beam.DoFn):

    BIRCH_MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
    DATA_ITEMS_SPEC = ReadModifyWriteStateSpec("data_items", PickleCoder())
    EMBEDDINGS_SPEC = ReadModifyWriteStateSpec("embeddings", PickleCoder())
    UPDATE_COUNTER_SPEC = ReadModifyWriteStateSpec("update_counter", PickleCoder())

{{< /highlight >}}

This example declares four different `ReadModifyWriteStateSpec objects`:

* `BIRCH_MODEL_SPEC` holds the state of clustering model.
* `DATA_ITEMS_SPEC` holds the Twitter texts seen so far.
* `EMBEDDINGS_SPEC` holds the normalized embeddings.
* `UPDATE_COUNTER_SPEC` holds the number of texts processed.


These `ReadModifyWriteStateSpec` objects are passed as an additional argument to the `process` function. When a news item comes in, we retrieve the existing state of the different objects, update them, and then write them back as persistent shared state.

{{< highlight file="sdks/python/apache_beam/examples/inference/online_clustering/clustering_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/online_clustering/clustering_pipeline/pipeline/transformations.py" stateful_clustering >}}
{{< /highlight >}}


`GetUpdates` is a `DoFn` that prints the cluster assigned to each Twitter message every time a new message arrives.

{{< highlight >}}
updated_clusters = clustering | "Format Update" >> beam.ParDo(GetUpdates())
{{< /highlight >}}