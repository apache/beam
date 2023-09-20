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
get data into the correct shape. The simplest of these is `MapToFields`
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

which includes all original fields *except* col3 in addition to outputting the
two new ones.


## Mapping functions

Of course one may want to do transformations beyond just dropping and renaming
fields.  Beam YAML has the ability to inline simple UDFs.
This requires a language specification. For example, using python
```
- type: MapToFields
  config:
    language: python
    fields:
      new_col: "col1.upper()"
      another_col: "col2 + col3"
```
Javascript ES5 is also supported, e.g.
```
- type: MapToFields
  config:
    language: javascript
    fields:
      new_col: "col1.toUpperCase()"
      another_col: "col2 === col3"
```

In addition, one can provide a full Python or Javascript callable that takes the row as an
argument to do more complex mappings
(see [PythonCallableSource](https://beam.apache.org/releases/pydoc/current/apache_beam.utils.python_callable.html#apache_beam.utils.python_callable.PythonCallableWithSource)
for acceptable formats). Using Python, one can write

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
Similarly, using Javascript, one can write
```
- type: MapToFields
  config:
    language: javascript
    fields:
      new_col:
        callable: |
          function myMapping(row) {
            if (row.col1.match("[0-9]+") && row.col2 > 0) {
              return "good"
            }
            else {
              return "bad"
            }
          }
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

The UDF can also be a separate file that is stored either on the local filesystem 
or in a GCS bucket.
Specifying the path to this UDF file and the name of the function is also supported.
```
- type: MapToFields
  config:
    language: python
    fields:
      new_col:
        path: /path/to/local/udf.py 
        name: my_mapping
      other_new_col:
        path: gs://my-gcs-bucket/path/to/gcs/udf.py
        name: my_other_mapping
```
where the UDF file looks something like
```
def my_mapping(row):
  return row.col2 > 0

def my_other_mapping(row):  
  return row.col2 > 1
```

Currently, in addition to Python and Javascript, SQL expressions are supported as well

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
noting that the specific field should be exploded, e.g.

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col: "[col1.upper(), col1.lower(), col1.title()]"
      another_col: "col2 + col3"
    explode: new_col
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
    explode: [new_col, another_col]
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
    explode: [new_col, another_col]
    cross_product: false
```

will only emit three.

If one is only exploding existing fields, a simpler `Explode` transform may be
used instead

```
- type: Explode
  config:
    explode: [col1]
```

## Filtering

Sometimes it can be desirable to only keep records that satisfy a certain
criteria. This can be accomplished by specifying a keep parameter, e.g.

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col: "col1.upper()"
      another_col: "col2 + col3"
    keep: "col2 > 0"
```

Like explode, there is a simpler `Filter` transform useful when no mapping is
being done

```
- type: Filter
  config:
    language: sql
    keep: "col2 > 0"
```
