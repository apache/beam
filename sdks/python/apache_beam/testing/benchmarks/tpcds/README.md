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

# Apache Beam Python TPC-DS Benchmark & Evaluation Suite

This package provides a Python TPC-DS benchmark and correctness suite that uses:
- **Cross-language Beam SQL Expansion Service** (`:sdks:java:extensions:sql:expansion-service:shadowJar`) via `apache_beam.transforms.sql.SqlTransform`
- **Prism Runner** via `PrismRunner`
- **Java TPC-DS schemas and queries** copied into `tpcds/build` via Gradle (`copyTpcdsResources`)
- **GCS TPC-DS datasets** (`gs://beam-tpcds/datasets/text/10MB`, `1GB`, `10GB`, `100GB`)

## Running via Gradle

The Gradle tasks in `:sdks:python:apache_beam:testing:benchmarks:tpcds` automatically copy the Java TPC-DS schemas and queries into `tpcds/build` and build the Java SQL expansion service shadow jar before running Python.

### 1. Run the Supported TPC-DS Queries on the 10MB Dataset

```bash
./gradlew :sdks:python:apache_beam:testing:benchmarks:tpcds:run \
  -Ptpcds.args="--queries=supported --data_size=10MB"
```

### 2. Run the 13 Canonical Multi-Table Queries

```bash
./gradlew :sdks:python:apache_beam:testing:benchmarks:tpcds:run \
  -Ptpcds.args="--queries=canonical13 --data_size=10MB"
```

### 3. Run Specific Queries on 1GB GCS Data

```bash
./gradlew :sdks:python:apache_beam:testing:benchmarks:tpcds:run \
  -Ptpcds.args="--queries=3,7,42,43,52,55,96 --data_size=1GB --results_directory=/tmp/tpcds_results"
```

### 4. Run the Pytest Evaluation Suite

```bash
./gradlew :sdks:python:apache_beam:testing:benchmarks:tpcds:pytest
```
