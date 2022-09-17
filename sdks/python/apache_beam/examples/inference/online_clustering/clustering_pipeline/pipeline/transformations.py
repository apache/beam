from collections import Counter, defaultdict

import apache_beam as beam
import numpy as np
import torch
from apache_beam.coders import PickleCoder
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from sklearn.cluster import Birch
from transformers import AutoTokenizer, DistilBertModel

import config as cfg

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


class ModelWrapper(DistilBertModel):
    def forward(self, **kwargs):
        output = super().forward(**kwargs)
        sentence_embedding = (
            self.mean_pooling(output, kwargs["attention_mask"]).detach().cpu().numpy()
        )
        return sentence_embedding

    # Mean Pooling - Take attention mask into account for correct averaging
    def mean_pooling(self, model_output, attention_mask):
        token_embeddings = model_output[
            0
        ]  # First element of model_output contains all token embeddings
        input_mask_expanded = (
            attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        )
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
            input_mask_expanded.sum(1), min=1e-9
        )


class NormalizeEmbedding(beam.DoFn):
    def process(self, element, *args, **kwargs):
        """
        For each element in the input PCollection, normalize the embedding vector, and
        yield a new element with the normalized embedding added
        Args:
          element: The element to be processed.
        """
        (text, id), prediction = element
        embedding = prediction.inference
        l2_norm = np.linalg.norm(embedding)
        yield {"text": text, "id": id, "embedding": embedding / l2_norm}


class Decode(beam.DoFn):
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

    BIRCH_MODEL_SPEC = ReadModifyWriteStateSpec("clustering_model", PickleCoder())
    DATA_ITEMS_SPEC = ReadModifyWriteStateSpec("data_items", PickleCoder())
    EMBEDDINGS_SPEC = ReadModifyWriteStateSpec("embeddings", PickleCoder())
    UPDATE_COUNTER_SPEC = ReadModifyWriteStateSpec("update_counter", PickleCoder())
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
            # print(f"Doc-Text: {docs[doc_id]}, cluster_label: {cluster_label}")
        print(label_items_map)
        print("\n\n\n\n")
        yield label_items_map
