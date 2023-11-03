# pylint: skip-file
from apache_beam.ml.inference.huggingface_inference import HuggingFaceModelHandlerTensor
from apache_beam.ml.transforms.base import EmbeddingConfig

from sentence_transformers import SentenceTransformer
from typing import Callable


def inference_fn(batch, model, *args, **kwargs):
  return model.encode(batch)


class SentenceTransformerEmbeddings(EmbeddingConfig):
  def __init__(
      self,
      model_uri: str,
      model_class: Callable = SentenceTransformer,
      **kwargs):

    super().__init__(**kwargs)
    self.model_uri = model_uri
    self.model_class = model_class

  def get_model_handler(self):
    return HuggingFaceModelHandlerTensor(
        model_class=self.model_class,
        model_uri=self.model_uri,
        device=self.device,
        inference_fn=inference_fn,
        inference_args=self.inference_args,
        load_model_args=self.load_model_args,
        min_batch_size=self.min_batch_size,
        max_batch_size=self.max_batch_size,
        large_model=self.large_model,
        **self.kwargs)
