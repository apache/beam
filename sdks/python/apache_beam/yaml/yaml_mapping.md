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

# Beam YAML mappings

Beam YAML has the ability to do simple transformations which can be used to
get data into the correct shape. The simplest of these is `MaptoFields`
which creates records with new fields defined in terms of the input fields.

## Field renames

To rename fields one can write

```
- type: MapToFields
  config:
    fields:
      new_col1: col1
      new_col2: col2
```

will result in an output where each record has two fields,
`new_col1` and `new_col2`, whose values are those of `col1` and `col2`
respectively.

One can specify the append parameter which indicates the original fields should
be retained similar to the use of `*` in an SQL select statement. For example

```
- type: MapToFields
  config:
    append: true
    fields:
      new_col1: col1
      new_col2: col2
```

will output records that have `new_col1` and `new_col2` as *additional*
fields.  When the append field is specified, one can drop fields as well, e.g.

```
- type: MapToFields
  config:
    append: true
    drop:
      - col3
    fields:
      new_col1: col1
      new_col2: col2
```

which includes all original fiels *except* col3 in addition to outputting the
two new ones.


## Mapping functions

Of course one may want to do transformations beyond just dropping and renaming
fields.  Beam YAML has the ability to inline simple UDFs.
This requires a language specification. For example

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col: "col1.upper()"
      another_col: "col2 + col3"
```

In addition, one can provide a full Python callable that takes the row as an
argument to do more complex mappings
(see [PythonCallableSource](https://beam.apache.org/releases/pydoc/current/apache_beam.utils.python_callable.html#apache_beam.utils.python_callable.PythonCallableWithSource)
for acceptable formats). Thus one can write

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col:
        callable: |
          import re
          def my_mapping(row):
            if re.match("[0-9]+", row.col1) and row.col2 > 0:
              return "good"
            else:
              return "bad"
```

Once one reaches a certain level of complexity, it may be preferable to package
this up as a dependency and simply refer to it by fully qualified name, e.g.

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col:
        callable: pkg.module.fn
```

Currently, in addition to Python, SQL expressions are supported as well

```
- type: MapToFields
  config:
    language: sql
    fields:
      new_col: "UPPER(col1)"
      another_col: "col2 + col3"
```

## FlatMap

Sometimes it may be desirable to emit more (or less) than one record for each
input record.  This can be accomplished by mapping to an iterable type and
following the mapping with an Explode operation, e.g.

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col: "[col1.upper(), col1.lower(), col1.title()]"
      another_col: "col2 + col3"
- type: Explode
  config:
    fields: new_col
```

will result in three output records for every input record.

If more than one record is to be exploded, one must specify whether the cross
product over all fields should be taken. For example

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col: "[col1.upper(), col1.lower(), col1.title()]"
      another_col: "[col2 - 1, col2, col2 + 1]"
- type: Explode
  config:
    fields: [new_col, another_col]
    cross_product: true
```

will emit nine records whereas

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col: "[col1.upper(), col1.lower(), col1.title()]"
      another_col: "[col2 - 1, col2, col2 + 1]"
- type: Explode
  config:
    fields: [new_col, another_col]
    cross_product: false
```

will only emit three.

The `Explode` operation can be used on its own if the field in question is
already an iterable type.

```
- type: Explode
  config:
    fields: [col1]
```

## Filtering

Sometimes it can be desirable to only keep records that satisfy a certain
criteria. This can be accomplished with a `Filter` transform, e.g.

```
- type: Filter
  config:
    language: sql
    keep: "col2 > 0"
```

## Types

Beam will try to infer the types involved in the mappings, but sometimes this
is not possible. In these cases one can explicitly denote the expected output
type, e.g.

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col:
        expression: "col1.upper()"
        output_type: string
```

The expected type is given in json schema notation, with the addition that
a top-level basic types may be given as a literal string rather than requiring
a `{type: 'basic_type_name'}` nesting.

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col:
        expression: "col1.upper()"
        output_type: string
      another_col:
        expression: "beam.Row(a=col1, b=[col2])"
        output_type:
          type: 'object'
          properties:
            a:
              type: 'string'
            b:
              type: 'array'
              items:
                type: 'number'
```

This can be especially useful to resolve errors involving the inability to
handle the `beam:logical:pythonsdk_any:v1` type.
