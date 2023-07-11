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


{{if (eq .Sdk "java")}}
```
PCollection<WeatherData> weatherData =
    p.apply(
        BigQueryIO.read(
                (SchemaAndRecord elem) -> {
                  GenericRecord record = elem.getRecord();
                  return new WeatherData(
                      (Long) record.get("year"),
                      (Long) record.get("month"),
                      (Long) record.get("day"),
                      (Double) record.get("max_temperature"));
                })
            .fromQuery(
                "SELECT year, month, day, max_temperature "
                    + "FROM [clouddataflow-readonly:samples.weather_stations] "
                    + "WHERE year BETWEEN 2007 AND 2009")
            .withCoder(AvroCoder.of(WeatherData.class)));

// We will send the weather data into different tables for every year.
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
```
fictional_characters_view = beam.pvalue.AsDict(
    pipeline | 'CreateCharacters' >> beam.Create([('Yoda', True),('Obi Wan Kenobi', True)]))

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