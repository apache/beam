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

"""Utility functions for table row inference pipeline.

This module provides helper functions for testing and deploying the
table row inference pipeline, including data generation, model creation,
and Pub/Sub resource management.
"""

import json
import logging
import pickle
import time
from typing import Optional

try:
  from google.cloud import pubsub_v1
  PUBSUB_AVAILABLE = True
except ImportError:
  PUBSUB_AVAILABLE = False
  logging.warning('google-cloud-pubsub not available')

try:
  import numpy as np
  from sklearn.linear_model import LinearRegression
  SKLEARN_AVAILABLE = True
except ImportError:
  SKLEARN_AVAILABLE = False
  logging.warning('sklearn not available')


def create_sample_model(output_path: str, num_features: int = 3):
  """Create and save a simple linear regression model for testing.

  Args:
    output_path: Path where to save the model (local or GCS)
    num_features: Number of input features
  """
  if not SKLEARN_AVAILABLE:
    raise ImportError('sklearn is required to create sample models')

  model = LinearRegression()

  X_train = np.random.randn(100, num_features)
  y_train = np.sum(X_train, axis=1) + np.random.randn(100) * 0.1

  model.fit(X_train, y_train)

  with open(output_path, 'wb') as f:
    pickle.dump(model, f)

  logging.info('Sample model saved to %s', output_path)


def generate_sample_data(num_rows: int = 100,
                         num_features: int = 3) -> list[dict]:
  """Generate sample table row data for testing.

  Args:
    num_rows: Number of rows to generate
    num_features: Number of features per row

  Returns:
    List of dictionaries representing table rows
  """
  if not SKLEARN_AVAILABLE:
    raise ImportError('numpy is required to generate sample data')

  data = []
  for i in range(num_rows):
    row = {'id': f'row_{i}', 'timestamp': time.time()}

    for j in range(num_features):
      row[f'feature{j+1}'] = float(np.random.randn())

    data.append(row)

  return data


def write_data_to_file(data: list[dict], output_path: str):
  """Write sample data to JSONL file.

  Args:
    data: List of data dictionaries
    output_path: Output file path
  """
  with open(output_path, 'w') as f:
    for row in data:
      f.write(json.dumps(row) + '\n')

  logging.info('Wrote %d rows to %s', len(data), output_path)


def publish_to_pubsub(
    project: str,
    topic_name: str,
    data: list[dict],
    rate_limit: Optional[float] = None):
  """Publish sample data to Pub/Sub topic.

  Args:
    project: GCP project ID
    topic_name: Pub/Sub topic name
    data: List of data dictionaries to publish
    rate_limit: Optional rate limit (rows per second)
  """
  if not PUBSUB_AVAILABLE:
    raise ImportError('google-cloud-pubsub is required for Pub/Sub operations')

  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic_name)

  delay = 1.0 / rate_limit if rate_limit else 0

  for i, row in enumerate(data):
    message = json.dumps(row).encode('utf-8')
    publisher.publish(topic_path, message)

    if (i + 1) % 100 == 0:
      logging.info('Published %d messages', i + 1)

    if delay > 0:
      time.sleep(delay)

  logging.info('Published %d messages to %s', len(data), topic_path)


def ensure_pubsub_topic(project: str, topic_name: str) -> str:
  """Create Pub/Sub topic if it doesn't exist.

  Args:
    project: GCP project ID
    topic_name: Pub/Sub topic name

  Returns:
    Full topic path
  """
  if not PUBSUB_AVAILABLE:
    raise ImportError('google-cloud-pubsub is required for Pub/Sub operations')

  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic_name)

  try:
    publisher.get_topic(request={'topic': topic_path})
    logging.info('Topic %s already exists', topic_name)
  except Exception:
    publisher.create_topic(name=topic_path)
    logging.info('Created topic %s', topic_name)

  return topic_path


def ensure_pubsub_subscription(
    project: str, topic_name: str, subscription_name: str) -> str:
  """Create Pub/Sub subscription if it doesn't exist.

  Args:
    project: GCP project ID
    topic_name: Pub/Sub topic name
    subscription_name: Subscription name

  Returns:
    Full subscription path
  """
  if not PUBSUB_AVAILABLE:
    raise ImportError('google-cloud-pubsub is required for Pub/Sub operations')

  publisher = pubsub_v1.PublisherClient()
  subscriber = pubsub_v1.SubscriberClient()

  topic_path = publisher.topic_path(project, topic_name)
  subscription_path = subscriber.subscription_path(project, subscription_name)

  try:
    subscriber.get_subscription(request={'subscription': subscription_path})
    logging.info('Subscription %s already exists', subscription_name)
  except Exception:
    subscriber.create_subscription(name=subscription_path, topic=topic_path)
    logging.info('Created subscription %s', subscription_name)

  return subscription_path


def cleanup_pubsub_resources(
    project: str, topic_name: str, subscription_name: Optional[str] = None):
  """Delete Pub/Sub topic and optionally subscription.

  Args:
    project: GCP project ID
    topic_name: Pub/Sub topic name
    subscription_name: Optional subscription name to delete
  """
  if not PUBSUB_AVAILABLE:
    raise ImportError('google-cloud-pubsub is required for Pub/Sub operations')

  publisher = pubsub_v1.PublisherClient()
  subscriber = pubsub_v1.SubscriberClient()

  if subscription_name:
    subscription_path = subscriber.subscription_path(project, subscription_name)
    try:
      subscriber.delete_subscription(
          request={'subscription': subscription_path})
      logging.info('Deleted subscription %s', subscription_name)
    except Exception as e:
      logging.warning('Failed to delete subscription: %s', e)

  topic_path = publisher.topic_path(project, topic_name)
  try:
    publisher.delete_topic(request={'topic': topic_path})
    logging.info('Deleted topic %s', topic_name)
  except Exception as e:
    logging.warning('Failed to delete topic: %s', e)


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)

  import argparse
  parser = argparse.ArgumentParser(
      description='Utility for table row inference pipeline')
  parser.add_argument(
      '--action',
      required=True,
      choices=[
          'create_model',
          'generate_data',
          'publish_data',
          'create_topic',
          'create_subscription',
          'cleanup'
      ],
      help='Action to perform')
  parser.add_argument('--project', help='GCP project ID')
  parser.add_argument('--topic', help='Pub/Sub topic name')
  parser.add_argument('--subscription', help='Pub/Sub subscription name')
  parser.add_argument('--output_path', help='Output path for model or data')
  parser.add_argument(
      '--num_rows', type=int, default=100, help='Number of rows to generate')
  parser.add_argument(
      '--num_features', type=int, default=3, help='Number of features per row')
  parser.add_argument(
      '--rate_limit', type=float, help='Rate limit for publishing (rows/sec)')

  args = parser.parse_args()

  if args.action == 'create_model':
    if not args.output_path:
      raise ValueError('--output_path required for create_model')
    create_sample_model(args.output_path, args.num_features)

  elif args.action == 'generate_data':
    if not args.output_path:
      raise ValueError('--output_path required for generate_data')
    data = generate_sample_data(args.num_rows, args.num_features)
    write_data_to_file(data, args.output_path)

  elif args.action == 'publish_data':
    if not args.project or not args.topic:
      raise ValueError('--project and --topic required for publish_data')
    data = generate_sample_data(args.num_rows, args.num_features)
    publish_to_pubsub(args.project, args.topic, data, args.rate_limit)

  elif args.action == 'create_topic':
    if not args.project or not args.topic:
      raise ValueError('--project and --topic required for create_topic')
    ensure_pubsub_topic(args.project, args.topic)

  elif args.action == 'create_subscription':
    if not args.project or not args.topic or not args.subscription:
      raise ValueError(
          '--project, --topic, and --subscription required for '
          'create_subscription')
    ensure_pubsub_subscription(args.project, args.topic, args.subscription)

  elif args.action == 'cleanup':
    if not args.project or not args.topic:
      raise ValueError('--project and --topic required for cleanup')
    cleanup_pubsub_resources(args.project, args.topic, args.subscription)
