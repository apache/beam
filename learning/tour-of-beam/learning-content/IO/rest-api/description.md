<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

### BigQuery API
The BigQuery Storage Write API is a unified data-ingestion API for BigQuery. It combines streaming ingestion and batch loading into a single high-performance API. You can use the Storage Write API to stream records into BigQuery in real time or to batch process an arbitrarily large number of records and commit them in a single atomic operation.

{{if (eq .Sdk "go")}}
```
tableRef := client.Dataset("fir").Table("xasw")
table := tableRef.Create(ctx, schema, &bigquery.TableMetadata{})
```
{{end}}

{{if (eq .Sdk "java")}}
```
WriteResult writeResult = pCollection.apply("Save Rows to BigQuery",
                BigQueryIO.writeTableRows()
                        .to("tess-372508.fir.xas")
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
        );
```
{{end}}
{{if (eq .Sdk "python")}}
```
client = bigquery.Client(project=project_id)
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)
table = client.get_table(table_ref)  # API call

# Write the data to BigQuery
errors = client.insert_rows(table, rows)
```
{{end}}