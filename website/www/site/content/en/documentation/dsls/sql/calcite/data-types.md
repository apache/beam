---
type: languages
title: "Beam Calcite SQL data types"
aliases: /documentation/dsls/sql/data-types/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Beam Calcite SQL data types

Beam SQL supports standard SQL scalar data types as well as extensions
including arrays, maps, and nested rows. This page documents supported
[Apache Calcite data types](https://calcite.apache.org/docs/reference.html#data-types) supported by Beam Calcite SQL.

In Java, these types are mapped to Java types large enough to hold the
full range of values.

{{< table >}}
| SQL Type  | Description  | Java class |
| --------- | ------------ | ---------- |
| TINYINT   | 1 byte signed integer in range -128 to 127                                 | java.lang.Byte    |
| SMALLINT  | 2 byte signed integer in range -32768 to 32767                             | java.lang.Short   |
| INTEGER   | 4 byte signed integer in range -2147483648 to 2147483647                   | java.lang.Integer |
| BIGINT    | 8 byte signed integer in range -9223372036854775808 to 9223372036854775807 | java.lang.Long    |
| FLOAT     | 4 byte floating point                                     | java.lang.Float  |
| DOUBLE    | 8 byte floating point                                     | java.lang.Double |
| DECIMAL   | Arbitrary precision decimal value | java.math.BigDecimal     |
| VARCHAR   | Arbitrary length string           | java.lang.String         |
| TIMESTAMP | Millisecond precision timestamp   | org.joda.ReadableInstant |
| ARRAY<type>     | Ordered list of values      | java.util.List |
| MAP<type, type> | Finite unordered map        | java.util.Map  |
| ROW<fields>     | Nested row                  | org.apache.beam.sdk.values.Row |
{{< /table >}}
