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

# OnlineClustering Example

The OnlineClustering example demonstrates how to setup a realtime clustering pipeline that can read text from PubSub, convert the text into an embedding using a language model, and cluster them using BIRCH.

### Dataset for Clustering
For the example, we use a dataset called [emotion](https://huggingface.co/datasets/emotion). It comprises of 20,000 English Twitter messages with 6 basic emotions: anger, fear, joy, love, sadness, and surprise. The dataset has three splits: train, validation and test. It is a supervised dataset as it contains the text and the category(class) of the dataset. This dataset can easily be accessed using [HuggingFace Datasets](https://huggingface.co/docs/datasets/index).

To have a better understanding of the dataset, here are some examples from the train split of the dataset:


| Text        | Type of emotion |
| :---        |    :----:   |
| im grabbing a minute to post i feel greedy wrong      | Anger       |
| i am ever feeling nostalgic about the fireplace i will know that it is still on the property   | Love        |
| ive been taking or milligrams or times recommended amount and ive fallen asleep a lot faster but i also feel like so funny | Fear |
| on a boat trip to denmark | Joy |
| i feel you know basically like a fake in the realm of science fiction | Sadness |
| i began having them several times a week feeling tortured by the hallucinations moving people and figures sounds and vibrations | Fear |

### Clustering Algorithm
For the clustering of tweets, we use an incremental clustering algorithm called BIRCH. It stands for balanced iterative reducing and clustering using hierarchies and is an unsupervised data mining algorithm used to perform hierarchical clustering over particularly large data-sets. An advantage of BIRCH is its ability to incrementally and dynamically cluster incoming, multi-dimensional metric data points in an attempt to produce the best quality clustering for a given set of resources (memory and time constraints).


## Ingestion to PubSub
We first ingest the data into [PubSub](https://cloud.google.com/pubsub/docs/overview) so that while clustering we can read the tweets from PubSub. PubSub is a messaging service for exchanging event data among applications and services. It is used for streaming analytics and data integration pipelines to ingest and distribute data.

The full example code for ingesting data to PubSub can be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/online_clustering/write_data_to_pubsub_pipeline/)

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

`main.py` contains the pipeline code and some additional function used for running the pipeline

### How to Run the Pipeline ?
First, make sure you have installed the required packages.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`


The `write_data_to_pubsub_pipeline` contains four different transforms:
1. Load emotion dataset using HuggingFace Datasets (We take samples from 3 classes instead of 6 for simplicity)
2. Associate each text with a unique identifier (UID)
3. Convert the text into a format PubSub is expecting
4. Write the formatted message to PubSub


## Clustering on Streaming Data

After having the data ingested to PubSub, we can now look into the second pipeline, where we read the streaming message from PubSub, convert the text to a embedding using a language model and cluster them using BIRCH.

The full example code for all the steps mentioned above can be found [here](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/inference/online_clustering/clustering_pipeline/).


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

`pipeline/transformations.py` contains the code for different `beam.DoFn` that are used in the pipeline

`pipeline/options.py` contains the pipeline options to configure the Dataflow pipeline

`config.py` defines some variables like GCP PROJECT_ID, NUM_WORKERS that are used multiple times

`setup.py` defines the packages/requirements for the pipeline to run

`main.py` contains the pipeline code and some additional function used for running the pipeline

### How to Run the Pipeline ?
First, make sure you have installed the required packages and you have pushed data to PubSub.

1. Locally on your machine: `python main.py`
2. On GCP for Dataflow: `python main.py --mode cloud`

The pipeline can be broken down into few simple steps:

1. Reading the message from PubSub
2. Converting the PubSub message into a PCollection of dictionary where key is UID and value is twitter text
3. Encoding the text into transformer-readable token ID integers using a tokenizer
4. Using RunInference to get the vector embedding from a Transformer based Language Model
5. Normalizing the embedding for Clustering
6. Performing BIRCH Clustering using Stateful Processing
7. Printing the texts assigned to clusters

The code snippet for first two steps of pipeline where message from PubSub is read and converted into a dictionary

{{< highlight >}}
    docs = (
        pipeline
        | "Read from PubSub"
        >> ReadFromPubSub(subscription=cfg.SUBSCRIPTION_ID, with_attributes=True)
        | "Decode PubSubMessage" >> beam.ParDo(Decode())
    )
{{< /highlight >}}


We now closely look at three important steps of pipeline where we tokenize the text, fed the tokenized text to get embedding from a Transformer based Language Model and performing clustering using [Stateful Processing](https://beam.apache.org/blog/stateful-processing/).


### Getting Embedding from a Language Model

In order to do clustering with text data, we first need to map the text into vectors of numerical values suitable for statistical analysis. We use a transformer based language model called [sentence-transformers/stsb-distilbert-base/stsb-distilbert-base](https://huggingface.co/sentence-transformers/stsb-distilbert-base). It maps sentences & paragraphs to a 768 dimensional dense vector space and can be used for tasks like clustering or semantic search. But, we first need to tokenize the text as the language model is expecting a tokenized input instead of raw text.

Tokenization can be seen as a preprocessing task as it transforms text in a way that it can be fed into the model for getting predictions.

{{< highlight >}}
    normalized_embedding = (
        docs
        | "Tokenize Text" >> beam.Map(tokenize_sentence)
{{< /highlight >}}

Here, `tokenize_sentence` is a function that takes a dictionary with a text and an id, tokenizes the text, and returns a tuple (text, id) and the tokenized output.


Tokenized output is then passed to the language model for getting the embeddings. For getting embeddings from language model, we use `RunInference()` from beam.

{{< highlight >}}
    | "Get Embedding" >> RunInference(KeyedModelHandler(model_handler))

{{< /highlight >}}

After getting the embedding for each twitter text, the embeddings are normalized as it helps to make better clusters.

{{< highlight >}}
    | "Normalize Embedding" >> beam.ParDo(NormalizeEmbedding())

{{< /highlight >}}


### StatefulOnlineClustering
As the data is coming in a streaming fashion, so to cluster them we need an iterative clustering algorithm like BIRCH. As, the algorithm is iterative, we need a mechanism to store the previous state so that when a twitter text arrives, it can be updated accordingly.  **Stateful Processing** enables a `DoFn` to have persistent state which can be read and written during the processing of each element. One can read about Stateful Processing in the official documentation from Beam: [Link](https://beam.apache.org/blog/stateful-processing/).

In this example, every time a new message is Read from PubSub, we retrieve the existing state of the clustering model, update it and write it back to the state.

{{< highlight >}}
    clustering = (
        normalized_embedding
        | "Map doc to key" >> beam.Map(lambda x: (1, x))
        | "StatefulClustering using Birch" >> beam.ParDo(StatefulOnlineClustering())
    )
{{< /highlight >}}

As BIRCH doesn't support parallelization, so we need to make sure that all the StatefulProcessing is taking place only by one worker. In order to do that, we use the `Beam.Map` to associate each text to the same key `1`.

`StatefulOnlineClustering` is a `DoFn` that an embedding of a text and updates the clustering model. For storing the state it uses `ReadModifyWriteStateSpec` state object that acts as a container for storage.

{{< highlight >}}

class StatefulOnlineClustering(beam.DoFn):

    BIRCH_MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
    DATA_ITEMS_SPEC = ReadModifyWriteStateSpec("data_items", PickleCoder())
    EMBEDDINGS_SPEC = ReadModifyWriteStateSpec("embeddings", PickleCoder())
    UPDATE_COUNTER_SPEC = ReadModifyWriteStateSpec("update_counter", PickleCoder())

{{< /highlight >}}

We declare four different `ReadModifyWriteStateSpec objects`:

* `BIRCH_MODEL_SPEC`: holds the state of clustering model
* `DATA_ITEMS_SPEC`: holds the twitter texts seen so far
* `EMBEDDINGS_SPEC`: holds the normalized embeddings
* `UPDATE_COUNTER_SPEC`: holds the number of texts processed


These `ReadModifyWriteStateSpec objects` are passed as an additional argument to the `process` function. When a news item comes in, we retrieve the existing state of the different objects, update them and then write them back as persistent shared state.

{{< highlight file="sdks/python/apache_beam/examples/inference/online_clustering/clustering_pipeline/pipeline/transformations.py" >}}
{{< code_sample "sdks/python/apache_beam/examples/inference/online_clustering/clustering_pipeline/pipeline/transformations.py" stateful_clustering >}}
{{< /highlight >}}


`GetUpdates` is a `DoFn` that prints the cluster assigned to each twitter message, every time a new message arrives.

{{< highlight >}}
updated_clusters = clustering | "Format Update" >> beam.ParDo(GetUpdates())
{{< /highlight >}}