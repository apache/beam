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

import logging

from apache_beam.utils import retry

# Protect against environments where bigquery library is not available.
try:
  from google.cloud import bigquery
except ImportError:
  bigquery = None


class GcpTestIOError(retry.PermanentException):
  """Basic GCP IO error for testing. Function that raises this error should
  not be retried."""
  pass


@retry.with_exponential_backoff(
    num_retries=3,
    retry_filter=retry.retry_on_server_errors_filter)
def delete_bq_table(project, dataset, table):
  """Delete a Biqquery table.

  Args:
    project: Name of the project.
    dataset: Name of the dataset where table is.
    table:   Name of the table.
  """
  logging.info('Clean up a Bigquery table with project: %s, dataset: %s, '
               'table: %s.', project, dataset, table)
  bq_dataset = bigquery.Client(project=project).dataset(dataset)
  if not bq_dataset.exists():
    raise GcpTestIOError('Failed to cleanup. Bigquery dataset %s doesn\'t '
                         'exist in project %s.' % (dataset, project))
  bq_table = bq_dataset.table(table)
  if not bq_table.exists():
    raise GcpTestIOError('Failed to cleanup. Bigquery table %s doesn\'t '
                         'exist in project %s, dataset %s.' %
                         (table, project, dataset))
  bq_table.delete()
  if bq_table.exists():
    raise RuntimeError('Failed to cleanup. Bigquery table %s still exists '
                       'after cleanup.' % table)
