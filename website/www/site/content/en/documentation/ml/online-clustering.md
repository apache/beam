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
For the example, we use a dataset called [emotion](https://huggingface.co/datasets/emotion). It comprises of 20000 English Twitter messages with 6 basic emotions: anger, fear, joy, love, sadness, and surprise. The dataset has three splits: train (for training), validation and test (for performance evaluation). It is a supervised dataset as it contains the text and the category(class) of the dataset. This dataset can easily be accessed using [HuggingFace Datasets](https://huggingface.co/docs/datasets/index).

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
In order to simulate the real-time scenario, we first ingest the data into [PubSub](https://cloud.google.com/pubsub/docs/overview) so that while clustering we can read the tweets from PubSub. PubSub is a messaging service for exchanging event data among applications and services. It is used for streaming analytics and data integration pipelines to ingest and distribute data.

The full example code for ingesting data to PubSub can be found [here](insert/url/)

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

`pipeline/utils.py` contains the code for loading the emotion dataset and selecting a subset based on different types of emotion and split of dataset.

```
    import numpy as np
    from datasets import load_dataset

    def get_dataset(categories: list, split: str = "train"):
    """
    It takes a list of categories and a split (train/test/dev) and returns the corresponding subset of
    the dataset

    Args:
        categories (list): list of emotion categories to use
        split (str): The split of the dataset to use. Can be either "train", "dev", or "test". Defaults to
    train

    Returns:
        A list of text and a list of labels
    """
    labels = ["sadness", "joy", "love", "anger", "fear", "surprise"]
    label_map = {class_name: class_id for class_id, class_name in enumerate(labels)}
    labels_subset = np.array([label_map[class_name] for class_name in categories])
    emotion_dataset = load_dataset("emotion")
    X, y = np.array(emotion_dataset[split]["text"]), np.array(
        emotion_dataset[split]["label"]
    )
    subclass_idxs = [idx for idx, label in enumerate(y) if label in labels_subset]
    X_subset, y_subset = X[subclass_idxs], y[subclass_idxs]
    return X_subset.tolist(), y_subset.tolist()
```

The WriteDataToPubSub pipeline contains four different transforms:
In order to ingest the data to PubSub we create a pipeline that performs three steps:
1. Load emotion dataset using HuggingFace Datasets (We take samples from 3 classes instead of 6 for simplicity)
2. Associate each text with a unique identifier
3. Convert the text into the PubSub message format.
4. Write the text to a PubSub Topic

<!-- 1. The first transform creates a PCollection from the in-memory list that we created using the `get_dataset` function.

```
    train_categories = ["joy", "love", "fear"]
    test_categories = train_categories + ["sadness"]
    train_data, train_labels = get_dataset(train_categories)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        docs = (
            pipeline
            | "Load Documents" >> beam.Create(train_data[:10])
```

2. The second transform `AssignUniqueID` assigns each text with a unique identifier so that every text can be uniquely identified. The value returned after this Transform is a PCollection<Dict(id:text)>. It takes each text from the list and associates it with a UID and converts into a dictionary where the key is the UID and the value is the text itself.

```
    | "Assign unique key" >> beam.ParDo(AssignUniqueID())

```

3. The third transform `ConvertToPubSubMessage` converts each dictionary from the PCollection into a format that PubSub is expecting.

```
    docs
    | "Convert to PubSub Message" >> beam.ParDo(ConvertToPubSubMessage())
```

4. The last transform `WriteToPubSub` writes the PCollection to a PubSub topic.

```

    | "Write to PubSub"
    >> WriteToPubSub(topic=cfg.TOPIC_ID, with_attributes=True)

``` -->

## Clustering on Streaming Data

After having the data ingested to PubSub, we can now look into the second pipeline, where we read the streaming message from PubSub, convert the text to a embedding using a language model and cluster them using BIRCH.

The full example code for all the steps mentioned above can be found [here](insert/url/).


The file structure for online_clustering pipeline is:

    write_data_to_pubsub_pipeline/
    ├── pipeline/
    │   ├── __init__.py
    │   ├── options.py
    │   └── transformations.py
    ├── __init__.py
    ├── config.py
    ├── main.py
    └── setup.py

The pipeline can be broken down into few simple steps:

1. Reading the message from PubSub
2. Converting the PubSub message into a PCollection of dictionary where the key is the UID and the value is the twitter text
3. Encoding the text into transformer-readable token ID integers  using a tokenizer
4. Using RunInference to get the vector embedding from a Transfomer based Language Model
5. Normalizing the embedding for Clustering as normalization helps.
6. Perfoming Clustering using Stateful Processing
7. Printing the items assigned to each Cluster

The code snippet for the first two steps of the pipeline:

```
    docs = (
        pipeline
        | "Read from PubSub"
        >> ReadFromPubSub(subscription=cfg.SUBSCRIPTION_ID, with_attributes=True)
        | "Decode PubSubMessage" >> beam.ParDo(Decode())
    )
```


We now closely look at the three most important steps of pipeline where we tokenize the text, fed the tokenized text to get embedding from a Transformer based Language Model and performing clustering using [Stateful Processing](https://beam.apache.org/blog/stateful-processing/).


### Getting Embedding from a Language Model

In order to do clustering with text data, we first need to turn the text into vectors of numerical values suitable for statistical analysis. We use a transformer based language model called [sentence-transformers/stsb-distilbert-base/stsb-distilbert-base](https://huggingface.co/sentence-transformers/stsb-distilbert-base). It maps sentences & paragraphs to a 768 dimensional dense vector space and can be used for tasks like clustering or semantic search. But, we first need to tokenize the text as the language model is expecting a tokenized input instead of raw text.

Tokenization can be seen as a preprocessing task as it transforms text in a way that it can be fed into the model for getting predictions.

```
    normalized_embedding = (
        docs
        | "Tokenize Text" >> beam.Map(tokenize_sentence)
```

Here, `tokenize_sentence` is a function that takes a dictionary with a text and an id, tokenizes the text, and returns a tuple of the text and id and the tokenized output.

<!-- ```
Tokenizer = AutoTokenizer.from_pretrained(cfg.TOKENIZER_NAME)

def tokenize_sentence(input_dict):
    """
    It takes a dictionary with a text and an id, tokenizes the text, and returns a tuple of the text and
    id and the tokenized text
    Args:
      input_dict: a dictionary with the text and id of the sentence
    Returns:
      A tuple of the text and id, and a dictionary of the tokens.
    """
    text, id = input_dict["text"], input_dict["id"]
    tokens = Tokenizer([text], padding=True, truncation=True, return_tensors="pt")
    tokens = {key: torch.squeeze(val) for key, val in tokens.items()}
    return (text, id), tokens

``` -->

Tokenized output is then passed to the language model for getting the embeddings. For getting embeddings from language model, we use `RunInference()` from beam.

```
    | "Get Embedding" >> RunInference(KeyedModelHandler(model_handler))

```

After getting the embedding for each twitter text, the embeddings are normalized as it helps to make better clusters.

```
    | "Normalize Embedding" >> beam.ParDo(NormalizeEmbedding())

```

<!-- Here, `NormalizeEmbedding` is a `DoFn`. For each element in the input PCollection, it normalizes the embedding vector, and yields a new element with the normalized embedding added. -->

### StatefulOnlineClustering
As the data is coming in a streaming fashion, so to cluster them we need an iterative clustering algorithm like BIRCH. As, the algorithm is iterative, we need a mechanism to store the previous state so that when a twitter text arrives, it can be updated accordingly.  ** Stateful Processing ** enables a `DoFn` with the ability to have a persistent state which can be read and written during the processing of each element. One can read about Stateful Processing in the official documentation from Beam: [Link](https://beam.apache.org/blog/stateful-processing/).

In this example, every time a new message is Read from PubSub, we retrieve the existing state of the clustering model, update it and write it back to the state.

```
    clustering = (
        normalized_embedding
        | "Map doc to key" >> beam.Map(lambda x: (1, x))
        | "StatefulClustering using Birch" >> beam.ParDo(StatefulOnlineClustering())
    )

```
As BIRCH doesn't support parallelization, so we need to make sure that all the StatefulProcessing is taking place only by one worker. In order to do that, we use the `Beam.Map` to associate each text to the same key `1`.

`StatefulOnlineClustering` is a `DoFn` that an embedding of a text and updates the clustering model. For storing the state it uses `ReadModifyWriteStateSpec` state object that acts as a container for storage.

```

class StatefulOnlineClustering(beam.DoFn):

    BIRCH_MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
    DATA_ITEMS_SPEC = ReadModifyWriteStateSpec("data_items", PickleCoder())
    EMBEDDINGS_SPEC = ReadModifyWriteStateSpec("embeddings", PickleCoder())
    UPDATE_COUNTER_SPEC = ReadModifyWriteStateSpec("update_counter", PickleCoder())

```

We declare four different `ReadModifyWriteStateSpec objects`:

* `BIRCH_MODEL_SPEC`: holds the state of clustering model
* `DATA_ITEMS_SPEC`: holds the twitter texts seen so far
* `EMBEDDINGS_SPEC`: holds the normalized embeddings
* `UPDATE_COUNTER_SPEC`: holds the number of texts processed


These `ReadModifyWriteStateSpec objects` are passed as an additional argument to the process fun, where whenever a news item comes in, we retrieve the existing state of the different objects, update it and then write it back.

```
    def process(
        self,
        element,
        model_state=beam.DoFn.StateParam(BIRCH_MODEL_SPEC),
        collected_docs_state=beam.DoFn.StateParam(DATA_ITEMS_SPEC),
        collected_embeddings_state=beam.DoFn.StateParam(EMBEDDINGS_SPEC),
        update_counter_state=beam.DoFn.StateParam(UPDATE_COUNTER_SPEC),
        *args,
        **kwargs,
    ):
        """
        Takes the embedding of a document and updates the clustering model
        Args:
          element: The input element to be processed.
          model_state: This is the state of the clustering model. It is a stateful parameter, which means
        that it will be updated after each call to the process function.
          collected_docs_state: This is a stateful dictionary that stores the documents that have been
        processed so far.
          collected_embeddings_state: This is a dictionary of document IDs and their embeddings.
          update_counter_state: This is a counter that keeps track of how many documents have been
        processed.
        """
        # 1. Initialise or load states
        clustering = model_state.read() or Birch(n_clusters=None, threshold=0.7)
        collected_documents = collected_docs_state.read() or dict()
        collected_embeddings = collected_embeddings_state.read() or dict()
        update_counter = update_counter_state.read() or Counter()

        # 2. Extract document, add to state, and add to clustering model
        _, doc = element
        doc_id = doc["id"]
        embedding_vector = doc["embedding"]
        collected_embeddings[doc_id] = embedding_vector
        collected_documents[doc_id] = {"id": doc_id, "text": doc["text"]}
        update_counter = len(collected_documents)

        clustering.partial_fit(np.atleast_2d(embedding_vector))

        # 3. Predict cluster labels of collected documents
        cluster_labels = clustering.predict(
            np.array(list(collected_embeddings.values()))
        )

        # 4. Update the states
        model_state.write(clustering)
        collected_docs_state.write(collected_documents)
        collected_embeddings_state.write(collected_embeddings)
        update_counter_state.write(update_counter)
        yield {
            "labels": cluster_labels,
            "docs": collected_documents,
            "id": list(collected_embeddings.keys()),
            "counter": update_counter,
        }

```

`GetUpdates` is a `DoFn` that prints the cluster assigned to each twitter message, every time a new message comes.

```
updated_clusters = clustering | "Format Update" >> beam.ParDo(GetUpdates())
```