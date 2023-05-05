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

"""Utility methods for testing on GCP."""

# pytype: skip-file

import logging
import secrets
import time

from apache_beam.io import filesystems
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.utils import retry

# Protect against environments where bigquery library is not available.
try:
  from google.api_core import exceptions as gexc
  from google.cloud import bigquery
except ImportError:
  gexc = None
  bigquery = None

_LOGGER = logging.getLogger(__name__)


class GcpTestIOError(retry.PermanentException):
  """Basic GCP IO error for testing. Function that raises this error should
  not be retried."""
  pass


@retry.with_exponential_backoff(
    num_retries=3, retry_filter=retry.retry_on_server_errors_filter)
def create_bq_dataset(project, dataset_base_name):
  """Creates an empty BigQuery dataset.

  Args:
    project: Project to work in.
    dataset_base_name: Prefix for dataset id.

  Returns:
    A ``google.cloud.bigquery.dataset.DatasetReference`` object pointing to the
    new dataset.
  """
  client = bigquery.Client(project=project)
  unique_dataset_name = '%s%d%s' % (
      dataset_base_name, int(time.time()), secrets.token_hex(3))
  dataset_ref = client.dataset(unique_dataset_name, project=project)
  dataset = bigquery.Dataset(dataset_ref)
  client.create_dataset(dataset)
  return dataset_ref


@retry.with_exponential_backoff(
    num_retries=3, retry_filter=retry.retry_on_server_errors_filter)
def delete_bq_dataset(project, dataset_ref):
  """Deletes a BigQuery dataset and its contents.

  Args:
    project: Project to work in.
    dataset_ref: A ``google.cloud.bigquery.dataset.DatasetReference`` object
      pointing to the dataset to delete.
  """
  client = bigquery.Client(project=project)
  client.delete_dataset(dataset_ref, delete_contents=True)


@retry.with_exponential_backoff(
    num_retries=3, retry_filter=retry.retry_on_server_errors_filter)
def delete_bq_table(project, dataset_id, table_id):
  """Delete a BiqQuery table.

  Args:
    project: Name of the project.
    dataset_id: Name of the dataset where table is.
    table_id: Name of the table.
  """
  _LOGGER.info(
      'Clean up a BigQuery table with project: %s, dataset: %s, '
      'table: %s.',
      project,
      dataset_id,
      table_id)
  client = bigquery.Client(project=project)
  table_ref = client.dataset(dataset_id).table(table_id)
  try:
    client.delete_table(table_ref)
  except gexc.NotFound:
    raise GcpTestIOError('BigQuery table does not exist: %s' % table_ref)


@retry.with_exponential_backoff(
    num_retries=3, retry_filter=retry.retry_on_server_errors_filter)
def delete_directory(directory):
  """Delete a directory in a filesystem.

  Args:
    directory: Full path to a directory supported by Beam filesystems (e.g.
      "gs://mybucket/mydir/", "s3://...", ...)
  """
  filesystems.FileSystems.delete([directory])


def write_to_pubsub(
    pub_client,
    topic_path,
    messages,
    with_attributes=False,
    chunk_size=100,
    delay_between_chunks=0.1):
  for start in range(0, len(messages), chunk_size):
    message_chunk = messages[start:start + chunk_size]
    if with_attributes:
      futures = [
          pub_client.publish(topic_path, message.data, **message.attributes)
          for message in message_chunk
      ]
    else:
      futures = [
          pub_client.publish(topic_path, message) for message in message_chunk
      ]
    for future in futures:
      future.result()
    time.sleep(delay_between_chunks)


def read_from_pubsub(
    sub_client,
    subscription_path,
    with_attributes=False,
    number_of_elements=None,
    timeout=None):
  if number_of_elements is None and timeout is None:
    raise ValueError("Either number_of_elements or timeout must be specified.")
  messages = []
  start_time = time.time()

  while ((number_of_elements is None or len(messages) < number_of_elements) and
         (timeout is None or (time.time() - start_time) < timeout)):
    try:
      response = sub_client.pull(
          subscription_path, max_messages=1000, retry=None, timeout=10)
    except (gexc.RetryError, gexc.DeadlineExceeded):
      continue
    ack_ids = [msg.ack_id for msg in response.received_messages]
    sub_client.acknowledge(subscription=subscription_path, ack_ids=ack_ids)
    for msg in response.received_messages:
      message = PubsubMessage._from_message(msg.message)
      if with_attributes:
        messages.append(message)
      else:
        messages.append(message.data)
  return messages
