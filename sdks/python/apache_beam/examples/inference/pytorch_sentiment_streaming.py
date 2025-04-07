# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A Streaming pipeline that uses RunInference to perform Sentiment analysis
with DistilBERT model.
It reads input lines from a Pub/Sub topic (populated from a GCS file),
performs sentiment classification using a DistilBERT model via RunInference,
and writes results to BigQuery.
Resources like Pub/Sub topic/subscription and BigQuery table cleanup are
handled programmatically.
"""

import argparse
import logging
from collections.abc import Iterable

import apache_beam as beam
import torch
import torch.nn.functional as F
from apache_beam.ml.inference.base import (
    KeyedModelHandler, PredictionResult, RunInference)
from apache_beam.ml.inference.pytorch_inference import (
    PytorchModelHandlerKeyedTensor)
from apache_beam.options.pipeline_options import (
    PipelineOptions, SetupOptions, StandardOptions)
from apache_beam.runners.runner import PipelineResult
from google.cloud import pubsub_v1, bigquery
from transformers import (
    DistilBertTokenizerFast,
    DistilBertForSequenceClassification,
    DistilBertConfig)


class SentimentPostProcessor(beam.DoFn):
  """Processes PredictionResult to extract sentiment label and confidence."""
  def __init__(self, tokenizer: DistilBertTokenizerFast):
    self.tokenizer = tokenizer

  def process(self, element: tuple[str, PredictionResult]) -> Iterable[dict]:
    text, prediction_result = element
    logits = prediction_result.inference['logits']
    probs = F.softmax(logits, dim=-1)
    predicted_class = torch.argmax(probs).item()
    confidence = probs[predicted_class].item()
    sentiment = 'POSITIVE' if predicted_class == 1 else 'NEGATIVE'
    yield {
        'text': text, 'sentiment': sentiment, 'confidence': float(confidence)
    }


def tokenize_text(text: str,
                  tokenizer: DistilBertTokenizerFast) -> tuple[str, dict]:
  """Tokenizes input text using the specified tokenizer."""
  tokenized = tokenizer(
      text,
      padding='max_length',
      truncation=True,
      max_length=128,
      return_tensors="pt")
  return text, {k: torch.squeeze(v) for k, v in tokenized.items()}


def parse_known_args(argv):
  """Parses command-line arguments for pipeline execution."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_table', required=True, help='Path to output BigQuery table')
  parser.add_argument(
      '--model_path',
      default='distilbert-base-uncased-finetuned-sst-2-english',
      help='Hugging Face model name or local path')
  parser.add_argument(
      '--model_state_dict_path',
      dest='model_state_dict_path',
      required=True,
      help="Path to the model's state_dict.")
  parser.add_argument(
      '--input', required=True, help='Path to input file on GCS')
  parser.add_argument(
      '--pubsub_topic',
      default='projects/apache-beam-testing/topics/test_sentiment_topic',
      help='Pub/Sub topic to publish text lines to')
  parser.add_argument(
      '--pubsub_subscription',
      default='projects/apache-beam-testing/subscriptions/'
      'test_sentiment_subscription',
      help='Pub/Sub subscription to read from')
  parser.add_argument(
      '--project', default='apache-beam-testing', help='GCP project ID')
  return parser.parse_known_args(argv)


def ensure_pubsub_resources(
    project: str, topic_path: str, subscription_path: str):
  """Ensures that the required Pub/Sub topic and subscription exist."""
  publisher = pubsub_v1.PublisherClient()
  subscriber = pubsub_v1.SubscriberClient()

  topic_name = topic_path.split("/")[-1]
  subscription_name = subscription_path.split("/")[-1]

  full_topic_path = publisher.topic_path(project, topic_name)
  full_subscription_path = subscriber.subscription_path(
      project, subscription_name)

  try:
    publisher.get_topic(request={"topic": full_topic_path})
  except Exception:
    publisher.create_topic(name=full_topic_path)

  try:
    subscriber.get_subscription(
        request={"subscription": full_subscription_path})
  except Exception:
    subscriber.create_subscription(
        name=full_subscription_path, topic=full_topic_path)


def cleanup_pubsub_resources(
    project: str, topic_path: str, subscription_path: str):
  """Deletes Pub/Sub topic and subscription created for the pipeline."""
  publisher = pubsub_v1.PublisherClient()
  subscriber = pubsub_v1.SubscriberClient()

  topic_name = topic_path.split("/")[-1]
  subscription_name = subscription_path.split("/")[-1]

  full_topic_path = publisher.topic_path(project, topic_name)
  full_subscription_path = subscriber.subscription_path(
      project, subscription_name)

  try:
    subscriber.delete_subscription(
        request={"subscription": full_subscription_path})
    print(f"Deleted subscription: {subscription_name}")
  except Exception as e:
    print(f"Failed to delete subscription: {e}")

  try:
    publisher.delete_topic(request={"topic": full_topic_path})
    print(f"Deleted topic: {topic_name}")
  except Exception as e:
    print(f"Failed to delete topic: {e}")


def delete_bigquery_table_data(project: str, table: str):
  """Deletes all rows from the specified BigQuery table
     with known sentiment values."""
  client = bigquery.Client(project=project)
  query = (
      f"DELETE FROM `{table}` "
      f"WHERE sentiment IN ('POSITIVE', 'NEGATIVE')")
  try:
    client.query(query).result()
    print(f"Deleted all rows with sentiment from BigQuery table: {table}")
  except Exception as e:
    print(f"Failed to delete BigQuery table rows: {e}")


def run(
    argv=None, save_main_session=True, test_pipeline=None) -> PipelineResult:
  known_args, pipeline_args = parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True

  model_handler = PytorchModelHandlerKeyedTensor(
      model_class=DistilBertForSequenceClassification,
      model_params={'config': DistilBertConfig(num_labels=2)},
      state_dict_path=known_args.model_state_dict_path)
  ensure_pubsub_resources(
      project=known_args.project,
      topic_path=known_args.pubsub_topic,
      subscription_path=known_args.pubsub_subscription)

  tokenizer = DistilBertTokenizerFast.from_pretrained(known_args.model_path)

  pipeline = test_pipeline or beam.Pipeline(options=pipeline_options)

  # 1. Load data pipeline: read lines from GCS file and send to Pub/Sub
  _ = (
      pipeline
      | 'ReadGCSFile' >> beam.io.ReadFromText(known_args.input)
      | 'FilterEmpty' >> beam.Filter(lambda line: line.strip())
      | 'ToBytes' >> beam.Map(lambda line: line.encode('utf-8'))
      |
      'PublishToPubSub' >> beam.io.WriteToPubSub(topic=known_args.pubsub_topic))

  # 2. Main Streaming pipeline: read from PubSub subscription, process, write
  # result to BigQuery output table
  _ = (
      pipeline
      | 'ReadFromPubSub' >>
      beam.io.ReadFromPubSub(subscription=known_args.pubsub_subscription)
      | 'DecodeText' >> beam.Map(lambda x: x.decode('utf-8'))
      | 'Tokenize' >> beam.Map(lambda text: tokenize_text(text, tokenizer))
      | 'WindowedOutput' >> beam.WindowInto(
          beam.window.FixedWindows(60),
          trigger=beam.trigger.AfterProcessingTime(30),
          accumulation_mode=beam.trigger.AccumulationMode.DISCARDING)
      | 'RunInference' >> RunInference(KeyedModelHandler(model_handler))
      | 'PostProcess' >> beam.ParDo(SentimentPostProcessor(tokenizer))
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
          known_args.output_table,
          schema='text:STRING, sentiment:STRING, confidence:FLOAT',
          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS))

  result = pipeline.run()
  result.wait_until_finish(duration=1800000)  # 30 min
  result.cancel()

  cleanup_pubsub_resources(
      project=known_args.project,
      topic_path=known_args.pubsub_topic,
      subscription_path=known_args.pubsub_subscription)
  delete_bigquery_table_data(
      project=known_args.project, table=known_args.output_table)
  return result


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
