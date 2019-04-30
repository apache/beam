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

# "Cookbook" Examples

This directory holds simple "cookbook" examples, which show how to define
commonly-used data analysis patterns that you would likely incorporate into a
larger Apache Beam pipeline. They include:

 <ul>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/BigQueryTornadoes.java">BigQueryTornadoes</a>
  &mdash; An example that reads the public samples of weather data from Google
  BigQuery, counts the number of tornadoes that occur in each month, and
  writes the results to BigQuery. Demonstrates reading/writing BigQuery,
  counting a <code>PCollection</code>, and user-defined <code>PTransforms</code>.</li>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/CombinePerKeyExamples.java">CombinePerKeyExamples</a>
  &mdash; An example that reads the public &quot;Shakespeare&quot; data, and for
  each word in the dataset that exceeds a given length, generates a string
  containing the list of play names in which that word appears.
  Demonstrates the <code>Combine.perKey</code>
  transform, which lets you combine the values in a key-grouped
  <code>PCollection</code>.
  </li>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/DistinctExample.java">DistinctExample</a>
  &mdash; An example that uses Shakespeare's plays as plain text files, and
  removes duplicate lines across all the files. Demonstrates the
  <code>Distinct</code>, <code>TextIO.Read</code>,
  and <code>TextIO.Write</code> transforms, and how to wire transforms together.
  </li>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/FilterExamples.java">FilterExamples</a>
  &mdash; An example that shows different approaches to filtering, including
  selection and projection. It also shows how to dynamically set parameters
  by defining and using new pipeline options, and use how to use a value derived
  by a pipeline. Demonstrates the <code>Mean</code> transform,
  <code>Options</code> configuration, and using pipeline-derived data as a side
  input.
  </li>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/JoinExamples.java">JoinExamples</a>
  &mdash; An example that shows how to join two collections. It uses a
  sample of the <a href="http://goo.gl/OB6oin">GDELT &quot;world event&quot;
  data</a>, joining the event <code>action</code> country code against a table
  that maps country codes to country names. Demonstrates the <code>Join</code>
  operation, and using multiple input sources.
  </li>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook/MaxPerKeyExamples.java">MaxPerKeyExamples</a>
  &mdash; An example that reads the public samples of weather data from BigQuery,
  and finds the maximum temperature (<code>mean_temp</code>) for each month.
  Demonstrates the <code>Max</code> statistical combination transform, and how to
  find the max-per-key group.
  </li>
  </ul>

See the [documentation](http://beam.apache.org/get-started/quickstart/) and the [Examples
README](../../../../../../../../README.md) for
information about how to run these examples.
