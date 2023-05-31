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
### BigQuery with table-schema

{{if (eq .Sdk "java")}}
In Apache Beam, the `BigQueryIO` package provides the ability to read from and write to Google `BigQuery`. To use this package, you need to define a table schema for your BigQuery table, which specifies the names, data types, and modes of the columns in the table.
```
type User struct {
	ID   int32  `bigquery:"id"`
	Name string `bigquery:"name"`
	Age  int32  `bigquery:"age"`
}

rows := bigqueryio.Read(s, bigquery.TableReference{ProjectID: projectID, DatasetID: datasetID, TableID: tableID},
		beam.WithSchema(User{}))
```
{{end}}

{{if (eq .Sdk "java")}}
`DynamicDestinations` is a feature provided by the `BigQueryIO` class in Apache Beam that allows you to write data to different BigQuery tables based on the input elements. The feature allows you to specify a function that takes an input element and returns the destination table information (table name, schema, etc) for that element.

`DynamicDestinations` interface provided by the `BigQueryIO` class in Apache Beam has three methods:

* `getDestination`: takes an input element and returns a TableDestination object, which contains the information about the destination table.
* `getTable`: It takes an input element and returns the table name as a string.
* `getSchema`: It takes a table name and returns the schema as a TableSchema object.

Here is an example of how you might use the `BigQueryIO.write()` method with DynamicDestinations to write data to different BigQuery tables based on the input elements:

```
weatherData.apply(
    BigQueryIO.<WeatherData>write()
        .to(
            new DynamicDestinations<WeatherData, Long>() {
              @Override
              public Long getDestination(ValueInSingleWindow<WeatherData> elem) {
                return elem.getValue().year;
              }

              @Override
              public TableDestination getTable(Long destination) {
                return new TableDestination(
                    new TableReference()
                        .setProjectId(writeProject)
                        .setDatasetId(writeDataset)
                        .setTableId(writeTable + "_" + destination),
                    "Table for year " + destination);
              }

              @Override
              public TableSchema getSchema(Long destination) {
                return new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema()
                                .setName("year")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("month")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("day")
                                .setType("INTEGER")
                                .setMode("REQUIRED"),
                            new TableFieldSchema()
                                .setName("maxTemp")
                                .setType("FLOAT")
                                .setMode("NULLABLE")));
              }
            })
        .withFormatFunction(
            (WeatherData elem) ->
                new TableRow()
                    .set("year", elem.year)
                    .set("month", elem.month)
                    .set("day", elem.day)
                    .set("maxTemp", elem.maxTemp))
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE));
```
{{end}}

{{if (eq .Sdk "python")}}
You can use the dynamic destinations feature to write elements in a PCollection to different BigQuery tables, possibly with different schemas.

The dynamic destinations feature groups your user type by a user-defined destination key, uses the key to compute a destination table and/or schema, and writes each group’s elements to the computed destination.

In addition, you can also write your own types that have a mapping function to TableRow, and you can use side inputs in all DynamicDestinations methods.

```
fictional_characters_view = beam.pvalue.AsDict(
    pipeline | 'CreateCharacters' >> beam.Create([('Yoda', True),
                                                  ('Obi Wan Kenobi', True)]))

def table_fn(element, fictional_characters):
  if element in fictional_characters:
    return 'my_dataset.fictional_quotes'
  else:
    return 'my_dataset.real_quotes'

quotes | 'WriteWithDynamicDestination' >> beam.io.WriteToBigQuery(
    table_fn,
    schema=table_schema,
    table_side_inputs=(fictional_characters_view, ),
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
```
{{end}}