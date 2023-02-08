#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# beam-playground:
#   name: rest-api-io
#   description: REST-API BigQueryIO example.
#   multifile: false
#   context_line: 34
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam


from google.cloud import bigquery

def write_to_bigquery(project_id, dataset_id, table_id, rows):
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)  # API call

    # Write the data to BigQuery
    errors = client.insert_rows(table, rows)
    if not errors:
        print("Data written to table {}".format(table_id))
    else:
        print("Errors occurred while writing data to table {}: {}".format(table_id, errors))

rows = [
    (1, "John Doe", 30),
    (2, "Jane Doe", 25),
    (3, "Jim Smith", 40),
]
write_to_bigquery("tess-372508", "fir", "xasw", rows)
