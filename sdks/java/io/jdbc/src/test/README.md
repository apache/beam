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

These are instructions for maintaining postgres as needed for Integration Tests (JdbcIOIT).

You can always ignore these instructions if you have your own postgres cluster to test against.

Setting up Postgres
-------------------
1. Setup kubectl so it is configured to work with your kubernetes cluster
1. Run the postgres setup script
    src/test/resources/kubernetes/setup.sh
1. Do the data loading - create the data store instance by following the instructions in JdbcTestDataSet

... and your postgres instances are set up!

