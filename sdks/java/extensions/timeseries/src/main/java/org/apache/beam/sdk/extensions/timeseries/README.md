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
# Time series extensions

A set of extensions that carry out aggregation tasks for time series data.
TODO : Detailed documentation. 

## Example Pipelines

The examples included under timeseries serve to demonstrate advanced functionality using BEAM keyed 
state API and timers. The example pipeline is run from TimeSeriesExample, which makes use of some 
dummy data.

The example is very verbose in its output and includes the transform DebugSortedResult which is not 
suitable for large data sets.

[TimeSeriesExample](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/timeseries)   

