---
title: "Schema Patterns"
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

# Schema Patterns

The samples on this page show you common patterns using Schemas. 
A Schema is a way to represent records with a fixed structure, they are useful as common beam sources produce JSON, Avro or database row objects all of which have a well defined structure. 
For more information, see the [programming guide section on Schemas](/documentation/programming-guide/#what-is-a-schema).

{{< language-switcher java >}}

## Using Joins

Beam supports equijoins on schema `PCollections` of Schemas where the join condition depends on the equality of a subset of fields. 

Consider using Join if you have multiple data sets that provide information about related things and their structure is known.

For example let's say we have two different files with user data: one file has names and email addresses; the other file has names and phone numbers.
You can join the two data sets using the name as a common key and the other data as the associated values.
After the join, you have one dataset that contains all the information (email address and phone numbers) associated with each name.

The following conceptual examples uses two input collections to show the mechanism of Join.

You can define the Schema and the schema `PCollection` and then perform join on the two `PCollections` using a [Join](https://beam.apache.org/releases/javadoc/2.21.0/org/apache/beam/sdk/schemas/transforms/Join.html). 

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" SchemaJoinPatternJoin >}}
{{< /highlight >}}

The result `Row` is of the type `Row: [Row(emailSchema), Row(phoneSchema)]`, and it can be converted to desired format as shown in the code snippet below.

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" SchemaJoinPatternFormat >}}
{{< /highlight >}}

