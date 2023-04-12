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

### Sql transform

In Apache Beam, you can use **SQL** transforms to query and manipulate your data within the pipeline using SQL-like syntax. This is particularly useful if you're already familiar with SQL and want to leverage that knowledge while processing data in your Apache Beam pipeline.

Apache Beam **SQL** is built on top of Calcite, an open-source SQL parser, and optimizer framework.

To use **SQL** transforms in Apache Beam, you'll need to perform the following steps:

Create a `PCollection` of rows. In Apache Beam, **SQL** operations are performed on `PCollection<Row>` objects. You'll need to convert your data into a `PCollection` of rows, which requires defining a schema that describes the structure of your data.

Apply the SQL transform. Once you have your `PCollection<Row>`, you can apply a SQL transform using the `SqlTransform.query()` method. You'll need to provide the SQL query you want to execute on your data.

{{if (eq .Sdk "java")}}
Add the necessary dependencies to your project. For Java, you'll need to include the `beam-sdks-java-extensions-sql` dependency in your build configuration.

```
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

// Define the schema for your data
Schema schema = Schema.builder()
        .addField("id", Schema.FieldType.INT32)
        .addField("name", Schema.FieldType.STRING)
        .build();

// Assume input is a PCollection<Row> with the above schema
PCollection<Row> input = ...;

// Apply the SQL transform
PCollection<Row> result = input.apply(
        SqlTransform.query("SELECT id, name FROM PCOLLECTION WHERE id > 100"));
```
{{end}}

{{if (eq .Sdk "python")}}
```
import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform

# Define a sample input data as a list of dictionaries
input_data = [
    {'id': 1, 'name': 'Alice'},
    {'id': 2, 'name': 'Bob'},
    {'id': 101, 'name': 'Carol'},
    {'id': 102, 'name': 'David'},
]

# Create a pipeline
with beam.Pipeline() as p:
    # Read input data and convert it to a PCollection of Rows
    input_rows = (
        p | 'Create input data' >> beam.Create(input_data)
        | 'Convert to Rows' >> beam.Map(lambda x: beam.Row(id=int(x['id']), name=str(x['name'])))
    )

    # Apply the SQL transform
    filtered_rows = input_rows | SqlTransform("SELECT id, name FROM PCOLLECTION WHERE id > 100")

    # Print the results
    filtered_rows | 'Print results' >> beam.Map(print)
```
{{end}}