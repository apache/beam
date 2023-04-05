---
title:  "DataFrame API Preview now Available!"
date: "2020-12-16T09:09:41-08:00"
categories:
  - blog
authors:
  - bhulette
  - robertwb
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

We're excited to announce that a preview of the Beam Python SDK's new DataFrame
API is now available in [Beam
2.26.0](/blog/beam-2.26.0/). Much like `SqlTransform`
([Java](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/extensions/sql/SqlTransform.html),
[Python](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.sql.html#apache_beam.transforms.sql.SqlTransform)),
the DataFrame API gives Beam users a way to express complex
relational logic much more concisely than previously possible.
<!--more-->

## A more expressive API
Beam's new DataFrame API aims to be compatible with the well known
[Pandas](https://pandas.pydata.org/pandas-docs/stable/index.html)
DataFrame API, with a few caveats detailed below. With this new API a simple
pipeline that reads NYC taxiride data from a CSV, performs a grouped
aggregation, and writes the output to CSV, can be expressed very concisely:

```
from apache_beam.dataframe.io import read_csv

with beam.Pipeline() as p:
  df = p | read_csv("gs://apache-beam-samples/nyc_taxi/2019/*.csv",
                    usecols=['passenger_count' , 'DOLocationID'])
  # Count the number of passengers dropped off per LocationID
  agg = df.groupby('DOLocationID').sum()
  agg.to_csv(output)
```

Compare this to the same logic implemented as a conventional Beam python
pipeline with a `CombinePerKey`:

```
with beam.Pipeline() as p:
  (p | beam.io.ReadFromText("gs://apache-beam-samples/nyc_taxi/2019/*.csv",
                            skip_header_lines=1)
     | beam.Map(lambda line: line.split(','))
     # Parse CSV, create key - value pairs
     | beam.Map(lambda splits: (int(splits[8] or 0),  # DOLocationID
                                int(splits[3] or 0))) # passenger_count
     # Sum values per key
     | beam.CombinePerKey(sum)
     | beam.MapTuple(lambda loc_id, pc: f'{loc_id},{pc}')
     | beam.io.WriteToText(known_args.output))
```

The DataFrame example is much easier to quickly inspect and understand, as it
allows you to concisely express grouped aggregations without using the low-level
`CombinePerKey`.

In addition to being more expressive, a pipeline written with the DataFrame API
can often be more efficient than a conventional Beam pipeline.  This is because
the DataFrame API defers to the very efficient, columnar Pandas implementation
as much as possible.

## DataFrames as a DSL
You may already be aware of [Beam
SQL](/documentation/dsls/sql/overview/), which is
a Domain-Specific Language (DSL) built with Beam's Java SDK. SQL is
considered a DSL because it's possible to express a full pipeline, including IOs
and complex operations, entirely with SQL. 

Similarly, the DataFrame API is a DSL built with the Python SDK. You can see
that the above example is written without traditional Beam constructs like IOs,
ParDo, or CombinePerKey. In fact the only traditional Beam type is the Pipeline
instance! Otherwise this pipeline is written completely using the DataFrame API.
This is possible because the DataFrame API doesn't just implement Pandas'
computation operations, it also includes IOs based on the Pandas native
implementations (`pd.read_{csv,parquet,...}` and `pd.DataFrame.to_{csv,parquet,...}`).

Like SQL, it's also possible to embed the DataFrame API into a larger pipeline
by using
[schemas](/documentation/programming-guide/#what-is-a-schema).
A schema-aware PCollection can be converted to a DataFrame, processed, and the
result converted back to another schema-aware PCollection.  For example, if you
wanted to use traditional Beam IOs rather than one of the DataFrame IOs you
could rewrite the above pipeline like this:

```
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection

with beam.Pipeline() as p:
  ...
  schema_pc = (p | beam.ReadFromText(..)
                 # Use beam.Select to assign a schema
                 | beam.Select(DOLocationID=lambda line: int(...),
                               passenger_count=lambda line: int(...)))
  df = to_dataframe(schema_pc)
  agg = df.groupby('DOLocationID').sum()
  agg_pc = to_pcollection(pc)

  # agg_pc has a schema based on the structure of agg
  (agg_pc | beam.Map(lambda row: f'{row.DOLocationID},{row.passenger_count}')
          | beam.WriteToText(..))
```

It's also possible to use the DataFrame API by passing a function to
[`DataframeTransform`](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.transforms.html#apache_beam.dataframe.transforms.DataframeTransform):

```
from apache_beam.dataframe.transforms import DataframeTransform

with beam.Pipeline() as p:
  ...
  | beam.Select(DOLocationID=lambda line: int(..),
                passenger_count=lambda line: int(..))
  | DataframeTransform(lambda df: df.groupby('DOLocationID').sum())
  | beam.Map(lambda row: f'{row.DOLocationID},{row.passenger_count}')
  ...
```

## Caveats
As hinted above, there are some differences between Beam's DataFrame API and the
Pandas API. The most significant difference is that the Beam  DataFrame API is
*deferred*, just like the rest of the Beam API. This means that you can't
`print()` a DataFrame instance in order to inspect the data, because we haven't
computed the data yet! The computation doesn't take place until the pipeline is
`run()`.  Before that, we only know about the shape/schema of the result (i.e.
the names and types of the columns), and not the result itself.

There are a few common exceptions you will likely see when attempting to use
certain Pandas operations:

- **NotImplementedError:** Indicates this is an operation or argument that we
  haven't had time to look at yet. We've tried to make as many Pandas operations
  as possible available in the Preview offering of this new API, but there's
  still a long tail of operations to go.
- **WontImplementError:** Indicates this is an operation or argument we do not
  intend to support in the near-term because it's incompatible with the Beam
  model. The largest class of operations that raise this error are those that
  are order sensitive (e.g. shift, cummax, cummin, head, tail, etc..). These
  cannot be trivially mapped to Beam because PCollections, representing
  distributed datasets, are unordered. Note that even some of these operations
  *may* get implemented in the future - we actually have some ideas for how we
  might support order sensitive operations - but it's a ways off.

Finally, it's important to note that this is a preview of a new feature that
will get hardened over the next few Beam releases. We would love for you to try
it out now and give us some feedback, but we do not yet recommend it for use in
production workloads.

## How to get involved
The easiest way to get involved with this effort is to try out DataFrames and
let us know what you think! You can send questions to user@beam.apache.org, or
file bug reports and feature requests in [jira](https://issues.apache.org/jira).
In particular, it would be really helpful to know if there's an operation we
haven't implemented yet that you'd find useful, so that we can prioritize it.

If you'd like to learn more about how the DataFrame API works under the hood and
get involved with the development we recommend you take a look at the
[design doc](https://s.apache.org/beam-dataframes)
and our [Beam summit
presentation](https://2020.beamsummit.org/sessions/simpler-python-pipelines/).
From there the best way to help is to knock out some of those not implemented
operations. We're coordinating that work in
[BEAM-9547](https://issues.apache.org/jira/browse/BEAM-9547).
