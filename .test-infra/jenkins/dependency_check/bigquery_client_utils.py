from __future__ import print_function
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

import datetime
import logging
from google.cloud import bigquery

logging.getLogger().setLevel(logging.INFO)

class BigQueryClientUtils:

  def __init__(self, project_id, dataset_id, table_id):
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.table_id = table_id
    self.bigquery_client = bigquery.Client(project_id)
    self.table_ref = self.bigquery_client.dataset(dataset_id).table(table_id)
    self.table = self.bigquery_client.get_table(self.table_ref)


  def query_dep_info_by_version(self, dep, version):
    """
    Query for dependency information of a specific version
    Args:
      dep, version
    Return:
      release_date, is_currently_used
    """
    query = """SELECT release_date, is_currently_used 
      FROM `{0}.{1}.{2}` 
      WHERE package_name=\'{3}\' AND version=\'{4}\'""".format(self.project_id,
                                                               self.dataset_id,
                                                               self.table_id,
                                                               dep.strip(),
                                                               version.strip())

    query_job = self.bigquery_client.query(query)
    rows = list(query_job)
    if len(rows) == 0:
      logging.info("Did not find record of dependency {0} with version {1}.".format(dep, version))
      return None, False
    assert len(rows) == 1
    logging.info("""Found record of dependency {0} with version {1}: 
    release date: {2}; is_currently_used: {3}.""".format(dep, version, rows[0]['release_date'], rows[0]['is_currently_used']))
    return rows[0]['release_date'], rows[0]['is_currently_used']


  def query_currently_used_dep_info_in_db(self, dep):
    """
    Query for the info of the currently used version of a specific dependency
    Args:
      dep
    Return:
      version, release_date
    """
    query = """SELECT version, release_date 
      FROM `{0}.{1}.{2}` 
      WHERE package_name=\'{3}\' AND is_currently_used=True""".format(self.project_id,
                                                                     self.dataset_id,
                                                                     self.table_id,
                                                                     dep.strip())

    query_job = self.bigquery_client.query(query)
    rows = list(query_job)
    if len(rows) == 0:
      return None, None
    assert len(rows) == 1
    return rows[0]['version'], rows[0]['release_date']


  def insert_dep_to_table(self, dep, version, release_date, is_currently_used=False):
    """
    Add a dependency with version and release date into bigquery table
    Args:
      dep, version, is_currently_used (default False)
    """
    query = """INSERT  
    `{0}.{1}.{2}` (package_name, version, release_date, is_currently_used)
    VALUES (\'{3}\', \'{4}\', \'{5}\', {6})""".format(self.project_id,
                                                  self.dataset_id,
                                                  self.table_id,
                                                  dep.strip(),
                                                  version.strip(),
                                                  release_date,
                                                  is_currently_used)
    logging.info("Inserting dep to table: \n {0}".format(query))
    try:
      query_job = self.bigquery_client.query(query)
      if not query_job.done():
        print(query_job.result())
    except:
      raise


  def delete_dep_from_table(self, dep, version):
    """
    Remove a dependency record from the table.
    Args:
      dep, version
    """
    query = """DELETE  
      FROM `{0}.{1}.{2}` 
      WHERE package_name=\'{3}\' AND version=\'{4}\'""".format(self.project_id,
                                                               self.dataset_id,
                                                               self.table_id,
                                                               dep.strip(),
                                                               version.strip())
    logging.info("Deleting dep from table: \n {0}".format(query))
    try:
      query_job = self.bigquery_client.query(query)
      if not query_job.done():
        print(query_job.result())
    except:
      raise


  def clean_stale_records_from_table(self):
    """
    Remove stale records from the table. A record is stale if it is not currently used and the release date is behind 3
    years.
    """
    query = """DELETE  
      FROM `{0}.{1}.{2}` 
      WHERE release_date < '{3}'""".format(self.project_id,
                                                self.dataset_id,
                                                self.table_id,
                                                datetime.datetime.today().date() - datetime.timedelta(3*365))
    logging.info("Clean Up Starts")
    try:
      query_job = self.bigquery_client.query(query)
      if not query_job.done():
        logging.error(query_job.result())
    except:
      raise
