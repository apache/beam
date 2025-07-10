---
type: languages
title: "Apache Beam YAML Join"
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

# Beam YAML Join

Beam YAML can join two or more inputs on specified columns. For example, the
following pipeline joins the First Input pcollection and Second Input
pcollection when col1 in First Input is equal to col2 in Second Input.

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
  config:
    equalities:
      - input1: col1
        input2: col2
```

When joining multiple inputs on one column that is named the same across all the
inputs, one can use the following  shorthand syntax:

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
    input3: Third Input
  config:
    equalities: col1
```

## Join Types

When using the Join transform, one can specify the type of join to perform on
the inputs. If no join type is specified, the inputs are all joined using an
inner join. The supported join types are:

| Join Type | YAML Keyword |
| -------- | ------- |
| Inner Join | inner |
| Full Outer Join | left |
| Right Outer Join | right |

The following example  joins two inputs  using an inner join on the specified
equalities:

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
  config:
    type: inner
    equalities:
      - input1: col1
        input2: col1
```


The following example joins two inputs using a left outer join on the specified
equalities. In this case, all rows from input1 will be kept because input1 is
the left input. Order of joins follows the sequence as specified in equalities.

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
  config:
    type: left
    equalities:
      - input1: col1
        input2: col1
```

The following example joins three inputs using an full outer join on the
specified equalities:

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
    input3: Third Input
  config:
    type: outer
    equalities:
      - input1: col1
        input2: col1
      - input2: col2
        input3: col2
```

If you want a combination of join types, you can specify the inputs to be outer
joined. The following  example joins input1 with input2 using a right outer join
since input2 is on the right side and will join input2 with input 3 using a left
outer join since input2 is on the left side.

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
    input3: Third Input
  config:
    type:
      outer:
        - input2
    equalities:
      - input1: col1
        input2: col1
      - input2: col2
        input3: col2
```

## Fields
By default, the join transform includes all columns from all input tables. If
column names clash, it's best to rename them explicitly. Otherwise, the system
will deduplicate names by adding a numeric suffix

To choose which columns to output, or to customize the output column names, use
the "fields" configuration.

To specify which columns to output from an input, use the input reference as the
configuration key and a list of desired columns as the configuration value. The
following example outputs col1 from input1, col2 and col3 from input2, and all
the columns from input 3. If there is a name clash, it appends a numeric suffix
to avoid duplicate naming.

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
    input3: Third Input
  config:
    equalities: col1
    fields:
      input1: [col1]
      input2: [col2, col3]
```

To rename a column in the output, create a mapping for the input with the key as
the new column name and the value as the original column name. The following
example maps col1 from input3 to the column name "renamed_col1":

```
- type: Join
  input:
    input1: First Input
    input2: Second Input
    input3: Third Input
  config:
    equalities: col1
    fields:
      input1: [col1]
      input2: [col2, col3]
      input3:
        renamed_col1: col1
```
