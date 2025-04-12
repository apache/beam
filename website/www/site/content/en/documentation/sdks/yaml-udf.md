---
type: languages
title: "Apache Beam YAML UDFs"
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
respectively (which are the names of two fields from the input schema).

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
This requires a language specification. For example, we can provide a
Python expression referencing the input fields

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
for acceptable formats). Thus, one can write

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

It is also possible to store the function logic in a file and point to the function name, e.g.

```
- type: MapToFields
  config:
    language: python
    fields:
      new_col:
        path: /path/to/some/udf.py
        name: my_mapping
```

Currently, in addition to Python, Java, SQL, and JavaScript (experimental)
expressions are supported as well

### Java

When using Java mappings, the UDF type must be declared, even for simple expressions, e.g.

```
- type: MapToFields
  config:
    language: java
    fields:
      new_col:
        expression: col1.toUpperCase()
```

For callable UDFs, Java requires that the function be declared as a class that implements
[`java.util.function.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html), e.g.

```
- type: MapToFields
  config:
    language: java
    fields:
      new_col:
        callable: |
          import org.apache.beam.sdk.values.Row;
          import java.util.function.Function;
          public class MyFunction implements Function<Row, String> {
            public String apply(Row row) {
              return row.getString("col1").toUpperCase();
            }
          }
```

### SQL

When SQL is used for a `MapToFields` UDF, it is essentially the SQL `SELECT` statement.

For example, the query `SELECT UPPER(col1) AS new_col, "col2 + col3" AS another_col FROM PCOLLECTION` would
look like:

```
- type: MapToFields
  config:
    language: sql
    fields:
      new_col: "UPPER(col1)"
      another_col: "col2 + col3"
```

keeping in mind that any fields not intended to be included in the output should be added to the `drop` field.

If one wanted to select a field that collides with a [reserved SQL keyword](https://calcite.apache.org/docs/reference.html#keywords), the field(s) must be surrounded in backticks. For example, say the incoming PCollection has a field "timestamp", one would have to write:

```
- type: MapToFields
  config:
    language: sql
    fields:
      new_col: "`timestamp`"
```

**Note**: the field mapping tags and fields defined in `drop` do not need to be escaped. Only the UDF itself
needs to be a valid SQL statement.


### Generic

If a language is not specified the set of expressions is limited to pre-existing
fields and integer, floating point, or string literals.  For example

```
- type: MapToFields
  config:
    fields:
      new_col: col1
      int_literal: 389
      float_litera: 1.90216
      str_literal: '"example"'  # note the double quoting
```

## FlatMap

Sometimes it may be desirable to emit more (or less) than one record for each
input record.  This can be accomplished by mapping to an iterable type and
following the mapping with an `Explode` operation, e.g.

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
    keep: "col2 > 0"
```

For anything more complicated than a simple comparison between existing
fields and numeric literals a `language` parameter must be provided, e.g.

```
- type: Filter
  config:
    language: python
    keep: "col2 + col3 > 0"
```

For more complicated filtering functions, one can provide a full Python callable that takes the row as an
argument to do more complex mappings
(see [PythonCallableSource](https://beam.apache.org/releases/pydoc/current/apache_beam.utils.python_callable.html#apache_beam.utils.python_callable.PythonCallableWithSource)
for acceptable formats). Thus, one can write

```
- type: Filter
  config:
    language: python
    keep:
      callable: |
        import re
        def my_filter(row):
          return re.match("[0-9]+", row.col1) and row.col2 > 0
```

Once one reaches a certain level of complexity, it may be preferable to package
this up as a dependency and simply refer to it by fully qualified name, e.g.

```
- type: Filter
  config:
    language: python
    keep:
      callable: pkg.module.fn
```

It is also possible to store the function logic in a file and point to the function name, e.g.

```
- type: Filter
  config:
    language: python
    keep:
      path: /path/to/some/udf.py
      name: my_filter
```

This allows the logic of the UDF itself to be more easily developed and tested
using standard software engineering practices.

Currently, in addition to Python, Java, SQL, and JavaScript (experimental)
expressions are supported as well

### Java

When using Java filtering, the UDF type must be declared, even for simple expressions, e.g.

```
- type: Filter
  config:
    language: java
    keep:
      expression: col2 > 0
```

For callable UDFs, Java requires that the function be declared as a class that implements
[`java.util.function.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html), e.g.

```
- type: Filter
  config:
    language: java
    keep:
      callable: |
        import org.apache.beam.sdk.values.Row;
        import java.util.function.Function;
        import java.util.regex.Pattern;
        public class MyFunction implements Function<Row, Boolean> {
          public Boolean apply(Row row) {
            Pattern pattern = Pattern.compile("[0-9]+");
            return pattern.matcher(row.getString("col1")).matches() && row.getInt64("col2") > 0;
          }
        }
```

### SQL

Similar to [Mapping Functions](#mapping-functions), when SQL is used for a `MapToFields` UDF, it is essentially theSQL `WHERE` statement.

For example, the query `SELECT * FROM PCOLLECTION WHERE col2 > 0` would
look like:

```
- type: Filter
  config:
    language: sql
    keep: "col2 > 0"
```

If one wanted to filter on a field that collides with a
[reserved SQL keyword](https://calcite.apache.org/docs/reference.html#keywords), the field(s) must be surrounded in
backticks. For example, say the incoming PCollection has a field "timestamp", one would have to write:

```
- type: Filter
  config:
    language: sql
    keep: "`timestamp` > 0"
```


## Partitioning

It can also be useful to send different elements to different places
(similar to what is done with side outputs in other SDKs).
While this can be done with a set of `Filter` operations, if every
element has a single destination it can be more natural to use a `Partition`
transform instead which sends every element to a unique output.
For example, this will send all elements where `col1` is equal to `"a"` to the
output `Partition.a`.

```
- type: Partition
  input: input
  config:
    by: col1
    outputs: ['a', 'b', 'c']

- type: SomeTransform
  input: Partition.a
  config:
    param: ...

- type: AnotherTransform
  input: Partition.b
  config:
    param: ...
```

One can also specify the destination as a function, e.g.

```
- type: Partition
  input: input
  config:
    by: "'even' if col2 % 2 == 0 else 'odd'"
    language: python
    outputs: ['even', 'odd']
```

One can optionally provide a catch-all output which will capture all elements
that are not in the named outputs (which would otherwise be an error):

```
- type: Partition
  input: input
  config:
    by: col1
    outputs: ['a', 'b', 'c']
    unknown_output: 'other'
```

Sometimes one wants to split a PCollection into multiple PCollections
that aren't necessarily disjoint.  To send elements to multiple (or no) outputs,
one could use an iterable column and precede the `Partition` with an `Explode`.

```
- type: Explode
  input: input
  config:
    fields: col1

- type: Partition
  input: Explode
  config:
    by: col1
    outputs: ['a', 'b', 'c']
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


## Dependencies

Often user defined functions need to rely on external dependencies.
These can be provided with a `dependencies` attribute in the transform
config. For example

```
- type: MapToFields
  config:
    language: python
    dependencies:
      - 'scipy>=1.15'
    fields:
      new_col:
        callable: |
          import scipy.special

          def func(t):
            return scipy.special.zeta(complex(1/2, t))
```

The dependencies are interpreted according to the language, e.g.
for Java one provides a list of maven identifiers and/or jars,
and for Python one provides a list of pypi package specifiers and/or sdist tarballs.
See also the full examples using
[java dependencies](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/elementwise/map_to_fields_with_java_deps.yaml)
and
[python dependencies](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/yaml/examples/transforms/elementwise/map_to_fields_with_deps.yaml).
