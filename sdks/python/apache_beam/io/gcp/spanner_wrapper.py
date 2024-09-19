#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import uuid

from apache_beam.utils import retry

try:
  from google.cloud import spanner

except ImportError:
  spanner = None

_LOGGER = logging.getLogger(__name__)
MAX_RETRIES = 3


class SpannerWrapper(object):
  TEMP_DATABASE_PREFIX = 'temp-'

  def __init__(self, project_id, temp_database_id=None):
    self._spanner_client = spanner.Client(project=project_id)
    self._spanner_instance = self._spanner_client.instance("beam-test")
    self._test_database = None

    if temp_database_id and temp_database_id.startswith(
        self.TEMP_DATABASE_PREFIX):
      raise ValueError(
          'User provided temp database ID cannot start with %r' %
          self.TEMP_DATABASE_PREFIX)

    if temp_database_id is not None:
      self._test_database = temp_database_id
    else:
      self._test_database = self._get_temp_database()

  def _get_temp_database(self):
    uniq_id = uuid.uuid4().hex[:10]
    return f'{self.TEMP_DATABASE_PREFIX}{uniq_id}'

  @retry.with_exponential_backoff(
      num_retries=MAX_RETRIES,
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _create_database(self):
    _LOGGER.info('Creating test database: %s' % self._test_database)
    instance = self._spanner_instance
    database = instance.database(
        self._test_database,
        ddl_statements=[
            '''CREATE TABLE tmp_table (
            UserId    STRING(256) NOT NULL,
            Key       STRING(1024)
        ) PRIMARY KEY (UserId)'''
        ])
    operation = database.create()
    _LOGGER.info('Creating database: Done! %s' % str(operation.result()))

  def _delete_database(self):
    if (self._spanner_instance):
      database = self._spanner_instance.database(self._test_database)
      database.drop()
