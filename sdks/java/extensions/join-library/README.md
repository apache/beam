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

Join-library
============

Join-library provides inner join, outer left and right join functions to
Apache Beam. The aim is to simplify the most common cases of join to a
simple function call.

The functions are generic so it supports join of any types supported by
Beam. Input to the join functions are PCollections of Key/Values. Both the
left and right PCollections need the same type for the key. All the join
functions return a Key/Value where Key is the join key and value is
a Key/Value where the key is the left value and right is the value.

In the cases of outer join, since null cannot be serialized the user have
to provide a value that represent null for that particular use case.

Example how to use join-library:

    PCollection<KV<String, String>> leftPcollection = ...
    PCollection<KV<String, Long>> rightPcollection = ...

    PCollection<KV<String, KV<String, Long>>> joinedPcollection =
      Join.innerJoin(leftPcollection, rightPcollection);
