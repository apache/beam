<!--
  ~  Licensed to the Apache Software Foundation (ASF) under one
  ~  or more contributor license agreements.  See the NOTICE file
  ~  distributed with this work for additional information
  ~  regarding copyright ownership.  The ASF licenses this file
  ~  to you under the Apache License, Version 2.0 (the
  ~  "License"); you may not use this file except in compliance
  ~  with the License.  You may obtain a copy of the License at
  ~
  ~      http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~  Unless required by applicable law or agreed to in writing, software
  ~  distributed under the License is distributed on an "AS IS" BASIS,
  ~  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~  See the License for the specific language governing permissions and
  ~  limitations under the License.
  -->

Welcome To Apache Beam
----------------------

Apache Beam is an open source, unified model for defining both batch and streaming data-parallel
processing pipelines. Using one of the open source Beam SDKs, you build a program that defines the 
pipeline. The pipeline is then executed by one of Beam’s supported distributed processing back-ends,
which include Apache Flink, Apache Spark, and Google Cloud Dataflow.

Beam is particularly useful for Embarrassingly Parallel data processing tasks, in which the problem 
can be decomposed into many smaller bundles of data that can be processed independently and in 
parallel. You can also use Beam for Extract, Transform, and Load (ETL) tasks and pure data 
integration. These tasks are useful for moving data between different storage media and data 
sources, transforming data into a more desirable format, or loading data onto a new system.

To learn more about Apache Beam, refer to 
[Apache Beam Overview](https://beam.apache.org/get-started/beam-overview/).

**Kata:** Your first kata is to create a simple pipeline that takes a hardcoded input element 
"Hello Beam".

<div class="hint">
  Hardcoded input can be created using
  <a href="https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.core.html#apache_beam.transforms.core.Create">
  Create</a>.
</div>

<div class="hint">
  Refer to the Beam Programming Guide
  <a href="https://beam.apache.org/documentation/programming-guide/#creating-pcollection-in-memory">
  "Creating a PCollection from in-memory data"</a> section for more information.
</div>
