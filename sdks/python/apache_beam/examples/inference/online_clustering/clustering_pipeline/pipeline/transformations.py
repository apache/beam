#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""This file contains the transformations and utility functions for
the online_clustering pipeline."""
from collections import Counter
from collections import defaultdict

import numpy as np
from sklearn.cluster import Birch

import apache_beam as beam
import config as cfg
import torch
from apache_beam.coders import PickleCoder
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from transformers import AutoTokenizer
from transformers import DistilBertModel

Tokenizer = AutoTokenizer.from_pretrained(cfg.TOKENIZER_NAME)


def tokenize_sentence(input_dict):
  """
    It takes a dictionary with a text and an id, tokenizes the text, and
    returns a tuple of the text and id and the tokenized text

    Args:
      input_dict: a dictionary with the text and id of the sentence

    Returns:
      A tuple of the text and id, and a dictionary of the tokens.
    """
  text, uid = input_dict["text"], input_dict["id"]
  tokens = Tokenizer([text], padding=True, truncation=True, return_tensors="pt")
  tokens = {key: torch.squeeze(val) for key, val in tokens.items()}
  return (text, uid), tokens


class ModelWrapper(DistilBertModel):
  """Wrapper to DistilBertModel to get embeddings when calling
    forward function."""
  def forward(self, **kwargs):
    output = super().forward(**kwargs)
    sentence_embedding = (
        self.mean_pooling(output,
                          kwargs["attention_mask"]).detach().cpu().numpy())
    return sentence_embedding

  # Mean Pooling - Take attention mask into account for correct averaging
  def mean_pooling(self, model_output, attention_mask):
    """
        The function calculates the mean of token embeddings

        Args:
          model_output: The output of the model.
          attention_mask: This is a tensor that contains 1s for all input tokens and
          0s for all padding tokens.

        Returns:
          The mean of the token embeddings.
        """
    token_embeddings = model_output[
        0]  # First element of model_output contains all token embeddings
    input_mask_expanded = (
        attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float())
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9)


class NormalizeEmbedding(beam.DoFn):
  """A DoFn for normalization of text embedding."""
  def process(self, element, *args, **kwargs):
    """
        For each element in the input PCollection, normalize the embedding vector, and
        yield a new element with the normalized embedding added
        Args:
          element: The element to be processed.
        """
    (text, uid), prediction = element
    embedding = prediction.inference
    l2_norm = np.linalg.norm(embedding)
    yield {"text": text, "id": uid, "embedding": embedding / l2_norm}


class Decode(beam.DoFn):
  """A DoFn for decoding PubSub message into a dictionary."""
  def process(self, element, *args, **kwargs):
    """
        For each element in the input PCollection, retrieve the id and decode the bytes into string

        Args:
          element: The element that is being processed.
        """
    yield {
        "text": element.data.decode("utf-8"),
        "id": element.attributes["id"],
    }


class StatefulOnlineClustering(beam.DoFn):
  """A DoFn for online clustering on vector embeddings."""

  BIRCH_MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
  DATA_ITEMS_SPEC = ReadModifyWriteStateSpec("data_items", PickleCoder())
  EMBEDDINGS_SPEC = ReadModifyWriteStateSpec("embeddings", PickleCoder())
  UPDATE_COUNTER_SPEC = ReadModifyWriteStateSpec(
      "update_counter", PickleCoder())

  # [START stateful_clustering]
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
          model_state: This is the state of the clustering model. It is a stateful parameter,
          which means that it will be updated after each call to the process function.
          collected_docs_state: This is a stateful dictionary that stores the documents that
          have been processed so far.
          collected_embeddings_state: This is a dictionary of document IDs and their embeddings.
          update_counter_state: This is a counter that keeps track of how many documents have been
        processed.
        """
    # 1. Initialise or load states
    clustering = model_state.read() or Birch(n_clusters=None, threshold=0.7)
    collected_documents = collected_docs_state.read() or {}
    collected_embeddings = collected_embeddings_state.read() or {}
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
        np.array(list(collected_embeddings.values())))

    # 4. Write states
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
    # [END stateful_clustering]


class GetUpdates(beam.DoFn):
  """A DoFn for printing the clusters and items belonging to each cluster."""
  def process(self, element, *args, **kwargs):
    """
        Prints and returns clusters with items contained in it
        """
    cluster_labels = element.get("labels")
    doc_ids = element.get("id")
    docs = element.get("docs")
    print(f"Update Number: {element.get('counter')}:::\n")
    label_items_map = defaultdict(list)
    for doc_id, cluster_label in zip(doc_ids, cluster_labels):
      label_items_map[cluster_label].append(docs[doc_id])
    print(label_items_map)
    print("\n\n\n\n")
    yield label_items_map
