---
title: "Data processing"
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

Several types of Apache Beam data processing are applicable to AI/ML projects:
- Data exploration: Learn about your data (properties, distributions, statistics) when you start to deploy your project or when the data changes.
- Data preprocessing: Transform your data so that it is ready to be used to train your model.
- Data postprocessing: After running inference, you might need to transform the output of your model so that it is meaningful.
- Data validation: Check the quality of your data to detect outliers and calculate standard deviations and class distributions.

Data processing can be grouped into two main topics. This example first examimes data exploration and then data pipelines in ML that use both data preprocessing and validation. Data postprocessing is not covered because it is similar to prepressing. Postprocessing differs only in the order and type of pipeline.

## Initial data exploration

[Pandas](https://pandas.pydata.org/) is a popular tool for performing data exploration. Pandas is a data analysis and manipulation tool for Python. It uses DataFrames, which is a data structure that contains two-dimensional tabular data and that provides labeled rows and columns for the data. The Apache Beam Python SDK provides a [DataFrame API](/documentation/dsls/dataframes/overview/) for working with Pandas-like DataFrame objects.

The Beam DataFrame API is intended to provide access to a familiar programming interface within an Apache Beam pipeline. This API allows you to perform data exploration. You can reuse the code for your data preprocessing pipeline. Using the DataFrame API, you can build complex data processing pipelines by invoking standard Pandas commands.

You can use the DataFrame API in combination with the [Beam interactive runner](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/runners/interactive/README.md) in a [JupyterLab notebook](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development). Use the notebook to iteratively develop pipelines and display the results of your individual pipeline steps.

The following is an example of data exploration in Apache Beam in a notebook:

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

For a full end-to-end example that implements data exploration and data preprocessing with Apache Beam and the DataFrame API for your AI/ML project, see the [Beam Dataframe API tutorial for AI/ML](https://github.com/apache/beam/tree/master/examples/notebooks/beam-ml/dataframe_api_preprocessing.ipynb).

## Data pipeline for ML
A typical data preprocessing pipeline consists of the following steps:
1. Read and write data: Read and write the data from your file system, database, or messaging queue. Apache Beam has a rich set of [IO connectors](/documentation/io/built-in/) for ingesting and writing data.
2. Data cleaning: Filter and clean your data before using it in your ML model. You might remove duplicate or irrelevant data, correct mistakes in your dataset, filter out unwanted outliers, or handle missing data.
3. Data transformations: Your data needs to fit the expected input your model needs to train. You might need to normalize, one-hot encode, scale, or vectorize your data.
4. Data enrichment: You might want to enrich your data with external data sources to make your data more meaningful or easier for an ML model to interpret. For example, you might want to transform a city name or address into a set of coordinates.
5. Data validation and metrics: Make sure your data adheres to a specific set of requirements that can be validated in your pipeline. Report metrics from your data, such as the class distributions.

You can use an Apache Beam pipeline to implement all of these steps. This example shows a pipeline that demonstrates all of the steps previously mentioned:

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
