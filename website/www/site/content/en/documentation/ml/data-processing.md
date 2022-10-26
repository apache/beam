---
title: "Overview"
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

# Data processing

There are several types of data processing in Beam that are applicable in any AI/ML project:
- Data exploration: getting to know your data (properties, distributions, statistics) at the start of the development of your project, or when there are significant changes to your data.
- Data preprocessing: transforming your data so that it is ready to be used for training your model.
- Data post-processing: after running inference, you might need to transform the output of your model so that it is meaningful
- Data validation: check the quality of your data such as detecting outliers and reporting on standard deviations and class distributions.

This can be grouped into two main topics. We will look at data exploration first and secondly at data pipelines in ML which consists of both data preprocessing and validation. Data post-processing is not discussed explicitly here as this is in essence the same as preprocessing, but differs in only the order and type of pipeline.

## Initial data exploration

A popular tool to perform data exploration is [Pandas](https://pandas.pydata.org/). Pandas is a data analysis and manipulation tool for Python. It uses DataFrames, which is a data structure that contains two-dimensional tabular data and provides labeled rows and columns for the data. The Apache Beam Python SDK provides a [DataFrame API](https://beam.apache.org/documentation/dsls/dataframes/overview/) for working with Pandas-like DataFrame objects.

The Beam DataFrame API is intended to provide access to a familiar programming interface within a Beam pipeline. This allows you to easily perform data exploration, and later on re-use the same code for your data preprocessing pipeline. This way you can build complex data processing pipelines by only invoking standard Pandas commands.

You can use the DataFrame API in combination with the [Beam interactive runner](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/interactive/README.md) in a [JupyterLab notebook](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development). This lets you iteratively develop pipelines and display the results of your individual pipeline steps.

An example of data exploration in Beam in a notebook:

```
import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib

p = beam.Pipeline(InteractiveRunner())
beam_df = p | beam.dataframe.io.read_csv(input_path)

# Investigate columns and data types
beam_df.dtypes

# Generate descriptive statistics
ib.collect(beam_df.describe())

# Investigate missing values
ib.collect(beam_df.isnull())
```

For a full end-to-end example on how to implement data exploration and data preprocessing with Beam and the DataFrame API for your AI/ML project, you can follow the [Beam Dataframe API tutorial for AI/ML](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml/dataframe_api_preprocessing.ipynb).

## Data pipeline for ML
A typical data preprocessing pipeline consists of the following steps:
1. Reading and writing data: read/write the data from your filesystem, database or messaging queue. Beam has a rich set of [IO connectors](https://beam.apache.org/documentation/io/built-in/) for ingesting and writing data.
2. Data cleaning: you typically want to filter and clean your data before using it for your ML model. Examples of this are to remove duplicate or irrelevant data, correct mistakes in your dataset, filter out unwanted outliers and handle missing data.
3. Data transformations: your data needs to fit the expected input your model needs to train. Examples of this are normalization, one-hot encoding, scaling and vectorizing.
4. Data enrichment: often you will want to enrich your data with external data sources to make your data more meaningful or more easy to interpret by an ML model. An example of this is to transform a city name or address into a coordinate.
5. Data validation & metrics: you also want to make sure your data adheres to a specific set of requirements that can be validated in your pipeline. And you might want to report some metrics from your data such as the class distributions.

All these steps can successfully be implemented in a Beam pipeline. This is a minimalistic example of such a pipeline that demonstrates all above mentioned steps:

```
import apache_beam as beam
from apache_beam.metrics import Metrics

with beam.Pipeline() as pipeline:
  # Create data
  input_data = (
      pipeline
      | beam.Create([
         {'age': 25, 'height': 176, 'weight': 60, 'city': 'London'},
         {'age': 61, 'height': 192, 'weight': 95, 'city': 'Brussels'},
         {'age': 48, 'height': 163, 'weight': None, 'city': 'Berlin'}]))

  # Clean data
  def filter_missing_data(row):
    return row['weight'] is not None

  cleaned_data = input_data | beam.Filter(filter_missing_data)

  # Transform data
  def scale_min_max_data(row):
    row['age'] = (row['age']/100)
    row['height'] = (row['height']-150)/50
    row['weight'] = (row['weight']-50)/50
    yield row

  transformed_data = cleaned_data | beam.FlatMap(scale_min_max_data)

  # Enrich data
  side_input = pipeline | beam.io.ReadFromText('coordinates.csv')
  def coordinates_lookup(row, coordinates):
    row['coordinates'] = coordinates.get(row['city'], (0, 0))
    del row['city']
    yield row

  enriched_data = (
      transformed_data
      | beam.FlatMap(coordinates_lookup, coordinates=beam.pvalue.AsDict(side_input)))

  # Metrics
  counter = Metrics.counter('main', 'counter')

  def count_data(row):
    counter.inc()
    yield row

  output_data = enriched_data | beam.FlatMap(count_data)

  # Write data
  output_data | beam.io.WriteToText('output.csv')
```
