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

from __future__ import absolute_import

import logging
import time

from apache_beam.io import filesystems
from apache_beam.utils import retry

# Protect against environments where bigquery library is not available.
try:
  from google.cloud import bigquery
  from google.cloud.exceptions import NotFound
except ImportError:
  bigquery = None
  NotFound = None


class GcpTestIOError(retry.PermanentException):
  """Basic GCP IO error for testing. Function that raises this error should
  not be retried."""
  pass


@retry.with_exponential_backoff(
    num_retries=3,
    retry_filter=retry.retry_on_server_errors_filter)
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
  unique_dataset_name = dataset_base_name + str(int(time.time()))
  dataset_ref = client.dataset(unique_dataset_name, project=project)
  dataset = bigquery.Dataset(dataset_ref)
  client.create_dataset(dataset)
  return dataset_ref


@retry.with_exponential_backoff(
    num_retries=3,
    retry_filter=retry.retry_on_server_errors_filter)
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
    num_retries=3,
    retry_filter=retry.retry_on_server_errors_filter)
def delete_bq_table(project, dataset_id, table_id):
  """Delete a BiqQuery table.

  Args:
    project: Name of the project.
    dataset_id: Name of the dataset where table is.
    table_id: Name of the table.
  """
  logging.info('Clean up a BigQuery table with project: %s, dataset: %s, '
               'table: %s.', project, dataset_id, table_id)
  client = bigquery.Client(project=project)
  table_ref = client.dataset(dataset_id).table(table_id)
  try:
    client.delete_table(table_ref)
  except NotFound:
    raise GcpTestIOError('BigQuery table does not exist: %s' % table_ref)


@retry.with_exponential_backoff(
    num_retries=3,
    retry_filter=retry.retry_on_server_errors_filter)
def delete_directory(directory):
  """Delete a directory in a filesystem.

  Args:
    directory: Full path to a directory supported by Beam filesystems (e.g.
      "gs://mybucket/mydir/", "s3://...", ...)
  """
  filesystems.FileSystems.delete([directory])
