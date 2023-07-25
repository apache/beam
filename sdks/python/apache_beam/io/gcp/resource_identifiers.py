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

"""Helper functions to generate resource labels strings for GCP entitites

These can be used on MonitoringInfo 'resource' labels.

See example entities:
    https://s.apache.org/beam-gcp-debuggability

For GCP entities, populate the RESOURCE label with the aip.dev/122 format:
https://google.aip.dev/122

If an official GCP format does not exist, try to use the following format.
    //whatever.googleapis.com/parents/{parentId}/whatevers/{whateverId}
"""


def BigQueryTable(project_id, dataset_id, table_id):
  return '//bigquery.googleapis.com/projects/%s/datasets/%s/tables/%s' % (
      project_id, dataset_id, table_id)


def GoogleCloudStorageBucket(bucket_id):
  return '//storage.googleapis.com/buckets/%s' % bucket_id


def DatastoreNamespace(project_id, namespace_id):
  return '//bigtable.googleapis.com/projects/%s/namespaces/%s' % (
      project_id, namespace_id)


def SpannerTable(project_id, database_id, table_id):
  return '//spanner.googleapis.com/projects/%s/topics/%s/tables/%s' % (
      project_id, database_id, table_id)


def SpannerSqlQuery(project_id, query_name):
  return '//spanner.googleapis.com/projects/%s/queries/%s' % (
      project_id, query_name)


def BigtableTable(project_id, instance_id, table_id):
  return '//bigtable.googleapis.com/projects/%s/instances/%s/tables/%s' % (
      project_id, instance_id, table_id)
