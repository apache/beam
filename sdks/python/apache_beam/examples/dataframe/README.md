<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Example DataFrame API Pipelines

This module contains example pipelines that use the [Beam DataFrame
API](https://beam.apache.org/documentation/dsls/dataframes/overview/).

## Pre-requisites

You must have `apache-beam>=2.30.0` installed in order to run these pipelines,
because the `apache_beam.examples.dataframe` module was added in that release.
Using the DataFrame API also requires a compatible pandas version to be
installed, see the
[documentation](https://beam.apache.org/documentation/dsls/dataframes/overview/#pre-requisites)
for details.

## Wordcount Pipeline

Wordcount is the "Hello World" of data analytic systems, so of course we
had to implement it for the Beam DataFrame API! See [`wordcount.py`](./wordcount.py) for the
implementation. Note it demonstrates how to integrate the DataFrame API with
a larger Beam pipeline by using [Beam
Schemas](https://beam.apache.org/documentation/programming-guide/#what-is-a-schema)
in conjunction with
[to_dataframe](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.convert.html#apache_beam.dataframe.convert.to_dataframe)
and
[to_pcollection](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.convert.html#apache_beam.dataframe.convert.to_pcollection).

### Running the pipeline

To run the pipeline locally:

```sh
python -m apache_beam.examples.dataframe.wordcount \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output counts
```

This will produce files like `counts-XXXXX-of-YYYYY` with contents like:
```
KING: 243
LEAR: 236
DRAMATIS: 1
PERSONAE: 1
king: 65
of: 447
Britain: 2
OF: 15
FRANCE: 10
DUKE: 3
...
```

## Taxi Ride Example Pipelines

[`taxiride.py`](./taxiride.py) contains implementations for two DataFrame pipelines that
process the well-known [NYC Taxi
dataset](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). These
pipelines don't use any Beam primitives. Instead they build end-to-end pipelines
using the DataFrame API, by leveraging [DataFrame
IOs](https://beam.apache.org/releases/pydoc/current/apache_beam.dataframe.io.html).

The module defines two pipelines. The `location_id_agg` pipeline does a grouped
aggregation on the drop-off location ID. The `borough_enrich` pipeline extends
this example by joining the zone lookup table to find the borough where each
drop off occurred, and aggregate per borough.

### Data
Some snapshots of NYC taxi data have been staged in
`gs://apache-beam-samples` for use with these example pipelines:

- `gs://apache-beam-samples/nyc_taxi/2017/yellow_tripdata_2017-*.csv`: CSV files
  containing taxi ride data for each month of 2017 (similar directories exist
  for 2018 and 2019).
- `gs://apache-beam-samples/nyc_taxi/misc/sample.csv`: A sample of 1 million
  records from the beginning of 2019. At ~85 MiB this is a manageable size for
  processing locally.
- `gs://apache-beam-samples/nyc_taxi/misc/taxi+_zone_lookup.csv`: Lookup table
  with information about Zone IDs. Used by the `borough_enrich` pipeline.

### Running `location_id_agg`
To run the aggregation pipeline locally, use the following command:
```sh
python -m apache_beam.examples.dataframe.taxiride \
  --pipeline location_id_agg \
  --input gs://apache-beam-samples/nyc_taxi/misc/sample.csv \
  --output aggregation.csv
```

This will write the output to files like `aggregation.csv-XXXXX-of-YYYYY` with
contents like:
```
DOLocationID,passenger_count
1,3852
3,130
4,7725
5,24
6,37
7,7429
8,24
9,180
10,938
...
```

### Running `borough_enrich`
To run the enrich pipeline locally, use the command:
```sh
python -m apache_beam.examples.dataframe.taxiride \
  --pipeline borough_enrich \
  --input gs://apache-beam-samples/nyc_taxi/misc/sample.csv \
  --output enrich.csv
```

This will write the output to files like `enrich.csv-XXXXX-of-YYYYY` with
contents like:
```
Borough,passenger_count
Bronx,13645
Brooklyn,70654
EWR,3852
Manhattan,1417124
Queens,81138
Staten Island,531
Unknown,28527
```

## Flight Delay pipeline (added in 2.31.0)
[`flight_delays.py`](./flight_delays.py) contains an implementation of
a pipeline that processes the flight ontime data from
`bigquery-samples.airline_ontime_data.flights`. It uses a conventional Beam
pipeline to read from BigQuery, apply a 24-hour rolling window, and define a
Beam schema for the data. Then it converts to DataFrames in order to perform
a complex aggregation using `GroupBy.apply`, and write the result out with
`to_csv`. Note that the DataFrame computation respects the 24-hour window
applied above, and results are partitioned into separate files per day.

### Running the pipeline
To run the pipeline locally:

```sh
python -m apache_beam.examples.dataframe.flight_delays \
  --start_date 2012-12-24 \
  --end_date 2012-12-25 \
  --output gs://<bucket>/<dir>/delays.csv \
  --project <gcp-project> \
  --temp_location gs://<bucket>/<dir>
```

Note a GCP `project` and `temp_location` are required for reading from BigQuery.

This will produce files like
`gs://<bucket>/<dir>/delays.csv-2012-12-24T00:00:00-2012-12-25T00:00:00-XXXXX-of-YYYYY`
with contents tracking average delays per airline on that day, for example:
```
airline,departure_delay,arrival_delay
EV,10.01901901901902,4.431431431431432
HA,-1.0829015544041452,0.010362694300518135
UA,19.142555438225976,11.07180570221753
VX,62.755102040816325,62.61224489795919
WN,12.074298711144806,6.717968157695224
...
```
