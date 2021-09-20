---
type: languages
title: "Beam DataFrames: Overview"
---
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

# Beam DataFrames overview

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/dataframes.ipynb" >}}

The Apache Beam Python SDK provides a DataFrame API for working with pandas-like [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) objects. The feature lets you convert a PCollection to a DataFrame and then interact with the DataFrame using the standard methods available on the pandas DataFrame API. The DataFrame API is built on top of the pandas implementation, and pandas DataFrame methods are invoked on subsets of the datasets in parallel. The big difference between Beam DataFrames and pandas DataFrames is that operations are deferred by the Beam API, to support the Beam parallel processing model. (To learn more about differences between the DataFrame implementations, see [Differences from pandas](/documentation/dsls/dataframes/differences-from-pandas/).)

You can think of Beam DataFrames as a domain-specific language (DSL) for Beam pipelines. Similar to [Beam SQL](https://beam.apache.org/documentation/dsls/sql/overview/), DataFrames is a DSL built into the Beam Python SDK. Using this DSL, you can create pipelines without referencing standard Beam constructs like [ParDo](https://beam.apache.org/documentation/transforms/python/elementwise/pardo/) or [CombinePerKey](https://beam.apache.org/documentation/transforms/python/aggregation/combineperkey/).

The Beam DataFrame API is intended to provide access to a familiar programming interface within a Beam pipeline. In some cases, the DataFrame API can also improve pipeline efficiency by deferring to the highly efficient, vectorized pandas implementation.

## What is a DataFrame?

If you’re new to pandas DataFrames, you can get started by reading [10 minutes to pandas](https://pandas.pydata.org/pandas-docs/stable/user_guide/10min.html), which shows you how to import and work with the `pandas` package. pandas is an open-source Python library for data manipulation and analysis. It provides data structures that simplify working with relational or labeled data. One of these data structures is the [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html), which contains two-dimensional tabular data and provides labeled rows and columns for the data.

## Using DataFrames

To use Beam DataFrames, you need to install Apache Beam version 2.26.0 or higher (for complete setup instructions, see the [Apache Beam Python SDK Quickstart](https://beam.apache.org/get-started/quickstart-py/)) and pandas version 1.0 or higher. You can use DataFrames as shown in the following example, which reads New York City taxi data from a CSV file, performs a grouped aggregation, and writes the output back to CSV:

{{< highlight py >}}
from apache_beam.dataframe.io import read_csv

with beam.Pipeline() as p:
  df = p | read_csv("gs://apache-beam-samples/nyc_taxi/misc/sample.csv")
  agg = df[['passenger_count', 'DOLocationID']].groupby('DOLocationID').sum()
  agg.to_csv('output')
{{< /highlight >}}

pandas is able to infer column names from the first row of the CSV data, which is where `passenger_count` and `DOLocationID` come from.

In this example, the only traditional Beam type is the `Pipeline` instance. Otherwise the example is written completely with the DataFrame API. This is possible because the Beam DataFrame API includes its own IO operations (for example, `read_csv` and `to_csv`) based on the pandas native implementations. `read_*` and `to_*` operations support file patterns and any Beam-compatible file system. The grouping is accomplished with a group-by-key, and arbitrary pandas operations (in this case, `sum`) can be applied before the final write that occurs with `to_csv`.

The Beam DataFrame API aims to be compatible with the native pandas implementation, with a few caveats detailed below in [Differences from standard pandas](/documentation/dsls/dataframes/differences-from-pandas/).

## Embedding DataFrames in a pipeline

To use the DataFrames API in a larger pipeline, you can convert a PCollection to a DataFrame, process the DataFrame, and then convert the DataFrame back to a PCollection. In order to convert a PCollection to a DataFrame and back, you have to use PCollections that have [schemas](https://beam.apache.org/documentation/programming-guide/#what-is-a-schema) attached. A PCollection with a schema attached is also referred to as a *schema-aware PCollection*. To learn more about attaching a schema to a PCollection, see [Creating schemas](https://beam.apache.org/documentation/programming-guide/#creating-schemas).

Here’s an example that creates a schema-aware PCollection, converts it to a DataFrame using `to_dataframe`, processes the DataFrame, and then converts the DataFrame back to a PCollection using `to_pcollection`:

<!-- TODO(BEAM-11480): Convert these examples to snippets -->
{{< highlight py >}}
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
...
    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(known_args.input)

    words = (
        lines
        | 'Split' >> beam.FlatMap(
            lambda line: re.findall(r'[\w]+', line)).with_output_types(str)
        # Map to Row objects to generate a schema suitable for conversion
        # to a dataframe.
        | 'ToRows' >> beam.Map(lambda word: beam.Row(word=word)))

    df = to_dataframe(words)
    df['count'] = 1
    counted = df.groupby('word').sum()
    counted.to_csv(known_args.output)

    # Deferred DataFrames can also be converted back to schema'd PCollections
    counted_pc = to_pcollection(counted, include_indexes=True)

    # Do something with counted_pc
    ...
{{< /highlight >}}

You can find the full wordcount example on
[GitHub](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/dataframe/wordcount.py),
along with other [example DataFrame pipelines](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/dataframe/).

It’s also possible to use the DataFrame API by passing a function to [`DataframeTransform`][pydoc_dataframe_transform]:

{{< highlight py >}}
from apache_beam.dataframe.transforms import DataframeTransform

with beam.Pipeline() as p:
  ...
  | beam.Select(DOLocationID=lambda line: int(..),
                passenger_count=lambda line: int(..))
  | DataframeTransform(lambda df: df.groupby('DOLocationID').sum())
  | beam.Map(lambda row: f"{row.DOLocationID},{row.passenger_count}")
  ...
{{< /highlight >}}

[`DataframeTransform`][pydoc_dataframe_transform] is similar to [`SqlTransform`][pydoc_sql_transform] from the [Beam SQL](https://beam.apache.org/documentation/dsls/sql/overview/) DSL. Where `SqlTransform` translates a SQL query to a PTransform, `DataframeTransform` is a PTransform that applies a function that takes and returns DataFrames. A `DataframeTransform` can be particularly useful if you have a stand-alone function that can be called both on Beam and on ordinary pandas DataFrames.

`DataframeTransform` can accept and return multiple PCollections by name and by keyword, as shown in the following examples:

{{< highlight py >}}
output = (pc1, pc2) | DataframeTransform(lambda df1, df2: ...)

output = {'a': pc, ...} | DataframeTransform(lambda a, ...: ...)

pc1, pc2 = {'a': pc} | DataframeTransform(lambda a: expr1, expr2)

{...} = {a: pc} | DataframeTransform(lambda a: {...})
{{< /highlight >}}

[pydoc_dataframe_transform]: https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.transforms.html#apache_beam.dataframe.transforms.DataframeTransform
[pydoc_sql_transform]: https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.sql.html#apache_beam.transforms.sql.SqlTransform

{{< button-colab url="https://colab.research.google.com/github/apache/beam/blob/master/examples/notebooks/tour-of-beam/dataframes.ipynb" >}}
