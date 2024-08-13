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
import apache_beam as beam

try:
  from google.cloud import spanner

except ImportError:
  spanner = None

_LOGGER = logging.getLogger(__name__)

class SpannerWrapper(object):
  TEST_DATABASE = None
  TEMP_DATABASE_PREFIX = 'temp-'
  _SPANNER_CLIENT = None
  _SPANNER_INSTANCE = None

  def __init__(self, project_id = "apache-beam-testing", temp_database_id = None):
    self._SPANNER_CLIENT = spanner.Client(project = project_id)
    self._SPANNER_INSTANCE = self._SPANNER_CLIENT.instance("beam-test")
    self.TEST_DATABASE = None

    if temp_database_id and temp_database_id.startswith(self.TEMP_DATABASE_PREFIX):
      raise ValueError(
        'User provided temp database ID cannot start with %r' %
         self.TEMP_DATABASE_PREFIX)

    if temp_database_id is not None:
      self.TEST_DATABASE = temp_database_id
    else:
      self.TEST_DATABASE = self._get_temp_database()

  def _get_temp_database(self):
        uniq_id = uuid.uuid4().hex[:10]
        return f'{self.TEMP_DATABASE_PREFIX}{uniq_id}'

  def _create_database(self):
    _LOGGER.info('Creating test database: %s' % self.TEST_DATABASE)
    instance = self._SPANNER_INSTANCE
    database = instance.database(
        self.TEST_DATABASE,
        ddl_statements = [
            '''CREATE TABLE Users (
            UserId    STRING(256) NOT NULL,
            Key       STRING(1024)
        ) PRIMARY KEY (UserId)'''
        ])
    operation = database.create()
    _LOGGER.info('Creating database: Done! %s' % str(operation.result()))

  @classmethod
  def _delete_database(self):
    if (self._SPANNER_INSTANCE):
      database = self._SPANNER_INSTANCE.database(self.TEST_DATABASE)
      database.drop()
