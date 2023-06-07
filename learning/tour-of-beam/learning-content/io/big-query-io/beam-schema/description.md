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
### BigQuery with beam-schema

The `useBeamSchema` method is a method provided by the BigQueryIO class in Apache Beam to specify whether to use Beam's internal schema representation or BigQuery's native table schema when reading or writing data to BigQuery.

When you set `useBeamSchema` to true, Beam will use its internal schema representation when reading or writing data to BigQuery. This allows for more flexibility when working with the data, as Beam's schema representation supports more data types and allows for more advanced schema manipulation.

When you set `useBeamSchema` to false, Beam will use the native table schema of the BigQuery table when reading or writing data. This can be useful when you want to ensure that the data is written to BigQuery in a format that is compatible with other tools that read from the same table.

Here is an example of how you might use the useBeamSchema method when reading data from a BigQuery table:

```
pipeline.apply("ReadFromBigQuery",
    BigQueryIO.write().to("mydataset.outputtable").useBeamSchema())
```

The `BigQueryIO.write()` method creates a `Write` transform that will write the data to a new BigQuery table. The `to()` method specifies the name of the output table, which in this case is "**mydataset.outputtable**".

The `useBeamSchema()` method is called on the `Write` transform to use the schema of the `PCollection` elements as the schema of the output table.