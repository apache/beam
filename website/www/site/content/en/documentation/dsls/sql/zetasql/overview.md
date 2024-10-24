---
type: languages
title: "Beam ZetaSQL overview"
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
# Beam ZetaSQL overview
Beam SQL supports a variant of the [ZetaSQL](https://github.com/google/zetasql) language. ZetaSQL is similar to the language in BigQuery's SQL framework. This Beam SQL dialect is especially useful in pipelines that [write to or read from BigQuery tables](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryIO.html).

Beam SQL has additional extensions leveraging Beam’s unified batch/streaming model and processing complex data types. You can use these extensions with all Beam SQL dialects, including Beam ZetaSQL.

## Query syntax
Query statements scan tables or expressions and return the computed result rows. For more information about query statements in Beam ZetaSQL, see the [Query syntax](/documentation/dsls/sql/zetasql/query-syntax) reference and [Function call rules](/documentation/dsls/sql/zetasql/syntax).

## Lexical structure
A Beam SQL statement comprises a series of tokens. For more information about tokens in Beam ZetaSQL, see the [Lexical structure](/documentation/dsls/sql/zetasql/lexical) reference.

## Data types
Beam SQL supports standard SQL scalar data types as well as extensions including arrays, maps, and nested rows. For more information about scalar data in Beam ZetaSQL, see the [Data types](/documentation/dsls/sql/zetasql/data-types) reference.

## Functions and operators
For a list of the built-in functions and operators supported in Beam ZetaSQL, see [SupportedZetaSqlBuiltinFunctions.java](https://github.com/apache/beam/blob/master/sdks/java/extensions/sql/zetasql/src/main/java/org/apache/beam/sdk/extensions/sql/zetasql/SupportedZetaSqlBuiltinFunctions.java) (commented-out entries are not yet supported). For documentation on how these functions work, see the [ZetaSQL functions and operators](https://github.com/google/zetasql/blob/master/docs/functions-and-operators.md) reference.
