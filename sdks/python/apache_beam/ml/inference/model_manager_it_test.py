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
    DUPLICATE_FACTOR = 2  # Increase to test larger inputs

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
      ] * DUPLICATE_FACTOR

      pcoll = pipeline | 'CreateInputs' >> beam.Create(examples)

      predictions = pcoll | 'RunInference' >> RunInference(model_handler)

      actual_labels = predictions | beam.Map(lambda x: x.inference['label'])

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
      ] * DUPLICATE_FACTOR

      assert_that(
          actual_labels, equal_to(expected_labels), label='CheckPredictions')

    @unittest.skipIf(not torch.cuda.is_available(), "No GPU detected")
    def test_sentiment_analysis_large_roberta_gpu(self):
      """
      Runs inference using a Large architecture (RoBERTa-Large, ~355M params).
      This tests if the GPU can handle larger weights and requires more VRAM.
      """

      model_handler = HuggingFacePipelineModelHandler(
          task="sentiment-analysis",
          model="Siebert/sentiment-roberta-large-english",
          device=0,
          inference_args={"batch_size": 2})

      DUPLICATE_FACTOR = 2

      with TestPipeline() as pipeline:
        examples = [
            "I absolutely love this product, it's a game changer!",
            "This is the worst experience I have ever had.",
            "Apache Beam scales effortlessly to massive datasets.",
            "I am somewhat annoyed by the delay.",
            "The nuanced performance of this large model is impressive.",
            "I regret buying this immediately.",
            "The sunset looks beautiful tonight.",
            "This documentation is sparse and misleading.",
            "Winning the championship felt surreal.",
            "I'm feeling very neutral about this whole situation."
        ] * DUPLICATE_FACTOR

        pcoll = pipeline | 'CreateInputs' >> beam.Create(examples)
        predictions = pcoll | 'RunInference' >> RunInference(model_handler)
        actual_labels = predictions | beam.Map(
            lambda x: x.inference[0]['label'])

        # Note: Larger models are often more accurate with nuance.
        # e.g. "somewhat annoyed" is confidently NEGATIVE.
        expected_labels = [
            'POSITIVE',  # love
            'NEGATIVE',  # worst
            'POSITIVE',  # scales effortlessly
            'NEGATIVE',  # annoyed
            'POSITIVE',  # impressive
            'NEGATIVE',  # regret
            'POSITIVE',  # beautiful
            'NEGATIVE',  # misleading
            'POSITIVE',  # surreal
            'NEGATIVE'  # "neutral"
        ] * DUPLICATE_FACTOR

        assert_that(
            actual_labels,
            equal_to(expected_labels),
            label='CheckPredictionsLarge')
