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
### Reading BigQuery query results

Apache Beam's `BigQueryIO` connector allows you to read data from `BigQuery` tables and use it as a source for your data pipeline. The `BigQueryIO.Read()` method is used to read data from a `BigQuery` table based on a **SQL query**.
The `BigQueryIO.Read()` method reads data from a `BigQuery` table in parallel by automatically splitting the query into smaller pieces and running each piece in a separate `BigQuery` job. This can improve performance for large tables, but can also increase the cost of running your pipeline.

{{if (eq .Sdk "go")}}
```
bigquery.NewClient(context.Background(), options).Read(p,
		bigquery.Query("SELECT max_temperature FROM `tess-372508.fir.xasw`"),
		bigquery.WithCoder(bigquery.Float64()))
```
{{end}}
{{if (eq .Sdk "java")}}
```
PCollection<Double> maxTemperatures =
    p.apply(
        BigQueryIO.read(
                (SchemaAndRecord elem) -> (Double) elem.getRecord().get("max_temperature"))
            .fromQuery(
                "SELECT max_temperature FROM `tess-372508.fir.xasw`")
            .usingStandardSql()
            .withCoder(DoubleCoder.of()));
```
{{end}}
{{if (eq .Sdk "python")}}
```
lines = p | 'ReadFromBigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT max_temperature FROM `tess-372508.fir.xasw`'))
```
{{end}}