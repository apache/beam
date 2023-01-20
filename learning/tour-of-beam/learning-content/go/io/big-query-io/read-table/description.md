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
### BigQuery reading table

`BigQueryIO` allows you to read from a `BigQuery` table and read the results. By default, Beam invokes a `BigQuery` export request when you apply a BigQueryIO read transform. readTableRows returns a PCollection of BigQuery TableRow objects. Each element in the `PCollection` represents a single row in the table. `Integer` values in the `TableRow` objects are encoded as strings to match `BigQuery`’s exported JSON format. This method is convenient, but can be 2-3 times slower in performance compared to `read(SerializableFunction)`.

```
 PCollection<MyData> rows =
        pipeline
            .apply(
                "Read from BigQuery query",
                BigQueryIO.readTableRows().from("tess-372508.fir.xasw")
```