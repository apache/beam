import unittest
import torch
import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
try:
  from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
except ImportError as e:
  raise unittest.SkipTest(
      "HuggingFace model handler dependencies are not installed")
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to


class HuggingFaceGpuTest(unittest.TestCase):

  # This decorator skips the test if you run it on a machine without a GPU
  @unittest.skipIf(
      not torch.cuda.is_available(), "No GPU detected, skipping GPU test")
  def test_sentiment_analysis_on_gpu_large_input(self):
    """
    Runs inference on a GPU (device=0) with a larger set of inputs.
    """
    model_handler = HuggingFacePipelineModelHandler(
        task="sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english",
        device=0,  # <--- This forces GPU usage
        inference_args={"batch_size": 4
                        }  # Optional: Control batch size sent to GPU
    )

    with TestPipeline() as pipeline:
      examples = [
          "I absolutely love this product, it's a game changer!",
          "This is the worst experience I have ever had.",
          "The weather is okay, but I wish it were sunnier.",
          "Apache Beam makes parallel processing incredibly efficient.",
          "I am extremely disappointed with the service.",
          "Logic and reason are the pillars of good debugging.",
          "I'm so happy today!",
          "This error message is confusing and unhelpful.",
          "The movie was fantastic and the acting was superb.",
          "I hate waiting in line for so long."
      ]

      pcoll = pipeline | 'CreateInputs' >> beam.Create(examples)

      predictions = pcoll | 'RunInference' >> RunInference(model_handler)

      actual_labels = predictions | beam.Map(lambda x: x.inference[0]['label'])

      expected_labels = [
          'POSITIVE',  # "love this product"
          'NEGATIVE',  # "worst experience"
          'NEGATIVE',  # "weather is okay, but..."
          'POSITIVE',  # "incredibly efficient"
          'NEGATIVE',  # "disappointed"
          'POSITIVE',  # "pillars of good debugging"
          'POSITIVE',  # "so happy"
          'NEGATIVE',  # "confusing and unhelpful"
          'POSITIVE',  # "fantastic"
          'NEGATIVE'  # "hate waiting"
      ]

      assert_that(
          actual_labels, equal_to(expected_labels), label='CheckPredictions')
