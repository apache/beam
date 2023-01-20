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