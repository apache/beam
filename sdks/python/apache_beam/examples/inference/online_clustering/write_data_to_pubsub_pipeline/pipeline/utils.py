import numpy as np
from datasets import load_dataset
import uuid
import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage


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
    emotion_dataset = load_dataset("emotion", download_mode="force_redownload")
    X, y = np.array(emotion_dataset[split]["text"]), np.array(
        emotion_dataset[split]["label"]
    )
    subclass_idxs = [idx for idx, label in enumerate(y) if label in labels_subset]
    X_subset, y_subset = X[subclass_idxs], y[subclass_idxs]
    return X_subset.tolist(), y_subset.tolist()


class AssignUniqueID(beam.DoFn):
    def process(self, element):
        id = str(uuid.uuid4())
        yield {"id": id, "text": element}


class ConvertToPubSubMessage(beam.DoFn):
    def process(self, element):
        yield PubsubMessage(
            data=element["text"].encode("utf-8"), attributes={"id": element["id"]}
        )
