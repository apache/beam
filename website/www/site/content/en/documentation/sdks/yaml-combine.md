---
type: languages
title: "Apache Beam YAML Aggregations"
---
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

# Beam YAML Aggregations

Beam YAML has the ability to do aggregations to group and combine
values across records. The is accomplished via the `Combine` transform type.

For example, one can write

```
- type: Combine
  config:
    group_by: col1
    combine:
      total:
        value: col2
        fn:
          type: sum
```

If the function has no configuration requirements, it can be provided directly
as a string

```
- type: Combine
  config:
    group_by: col1
    combine:
      total:
        value: col2
        fn: sum
```

This can be simplified further if the output field name is the same as the input
field name

```
- type: Combine
  config:
    group_by: col1
    combine:
      col2: sum
```

One can aggregate over many fields at once

```
- type: Combine
  config:
    group_by: col1
    combine:
      col2: sum
      col3: max
```

and/or group by more than one field

```
- type: Combine
  config:
    group_by: [col1, col2]
    combine:
      col3: sum
```

or none at all (which will result in a global combine with a single output)

```
- type: Combine
  config:
    group_by: []
    combine:
      col2: sum
      col3: max
```

## Windowed aggregation

As with all transforms, `Combine` can take a windowing parameter

```
- type: Combine
  windowing:
    type: fixed
    size: 60s
  config:
    group_by: col1
    combine:
      col2: sum
      col3: max
```

If no windowing specification is provided, it inherits the windowing
parameters from upstream, e.g.

```
- type: WindowInto
  windowing:
    type: fixed
    size: 60s
- type: Combine
  config:
    group_by: col1
    combine:
      col2: sum
      col3: max
```

is equivalent to the previous example.


## Custom aggregation functions

One can use aggregation functions defined in Python by setting the language
parameter.

```
- type: Combine
  config:
    language: python
    group_by: col1
    combine:
      biggest:
        value: "col2 + col2"
        fn:
          type: 'apache_beam.transforms.combiners.TopCombineFn'
          config:
            n: 10
```

## SQL-style aggregations

By setting the language to SQL, one can provide full SQL snippets as the
combine fn.

```
- type: Combine
  config:
    language: sql
    group_by: col1
    combine:
      num_values: "count(*)"
      total: "sum(col2)"
```

One can of course also use the `Sql` transform type and provide a query
directly.
