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

# Twitter Connector

This directory contains an example pipelines for how to perform continues stream of data from twitter streaming api ( or any other 3rd party API ). This include:

<ul>
  <li><a href="https://javadoc.io/static/org.apache.beam/beam-sdks-java-core/current/org/apache/beam/sdk/transforms/DoFn.ProcessElement.html">Splitable Dofn</a>
  &mdash; A simple example of implementation of splittable dofn on an unbounded source with a simple incrementing watermarking logic.</li>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/twitterstreamgenerator/TwitterConnection.java">Connection Management</a>
  &mdash; The streaming pipeline example makes sure that only one Twitter connection is active at a time for a configuration.
  </li>
  <li><a href="https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete/twitterstreamgenerator/ReadFromTwitterDoFn.java">Terminating pipeline by time or elements</a>
  &mdash; The streaming pipeline keeps track of time and data collecting so far and terminated when the limit specified in passed.
  </li>
</ul>

## Requirements

- Java 8
- Twitter developer app account and streaming credentials.
- Direct runner or Flink runner.

This section describes what is needed to get the example up and running.

- Gradle preparation
- Local execution