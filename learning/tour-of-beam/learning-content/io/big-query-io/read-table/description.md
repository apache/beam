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
### Reading BigQuery table

`BigQueryIO` allows you to read from a `BigQuery` table and read the results. By default, Beam invokes a `BigQuery` export request when you apply a `BigQueryIO` read transform. In Java Beam SDK, readTableRows returns a `PCollection` of `BigQuery` `TableRow` objects. Each element in the `PCollection` represents a single row in the table.

> `Integer` values in the `TableRow` objects are encoded as strings to match `BigQuery`’s exported JSON format. This method is convenient but has a performance impact. Alternatively, you can use `read(SerializableFunction)` method to avoid this.

{{if (eq .Sdk "go")}}

```
rows := bigqueryio.Read(s, project, "bigquery-public-data:baseball.schedules", reflect.TypeOf(Game{}))
```

The `bigqueryio.Read()` method is called with a `bigquery.TableReference` object that specifies the project, dataset, and table IDs for the `BigQuery` table to read from.

The `Read()` method returns a PCollection of `TableRow` objects, which represent the rows of data in the BigQuery table.

The `ParDo()` method is called on the `PCollection` to apply a custom `DoFn` to each element in the collection. The `&logOutput{}` parameter specifies an instance of the `logOutput` struct to use as the `DoFn`.

The `logOutput` struct is defined as a custom `DoFn` that implements the ProcessElement method. This method takes a single `TableRow` object as input and logs its contents using the `log.Printf()` function.

{{end}}
{{if (eq .Sdk "java")}}
```
 PCollection<TableRow> pCollection = pipeline
                .apply("ReadFromBigQuery", BigQueryIO.readTableRows().from("clouddataflow-readonly:samples.weather_stations").withMethod(TypedRead.Method.DIRECT_READ))
```

The `BigQueryIO.readTableRows()` method is called to create a `BigQueryIO.Read` transform that will read data from a `BigQuery` table.

The `.from()` method is called on the `Read` transform to specify the name of the `BigQuery` table to read from. In this example, the table is named **tess-372508.fir.xasw**.

The `Read` transform returns a `PCollection` of `TableRow` objects, which represent the rows of data in the `BigQuery` table
{{end}}
{{if (eq .Sdk "python")}}
```
p | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(table='apache-beam-testing:clouddataflow_samples.weather_stations',
                                                            method=beam.io.ReadFromBigQuery.Method.DIRECT_READ)
```

The `beam.io.ReadFromBigQuery()` method is called to create a `Read` transform that will read data from a `BigQuery` table. The `table` parameter specifies the name of the `BigQuery` table to read from, along with any other configuration options such as **project ID**, **dataset ID**, or **query**.

The Read transform returns a `PCollection` of dict objects, where each dictionary represents a single row of data in the `BigQuery` table.
{{end}}