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

The samples on this page describe common patterns using Schemas. 
Schemas provide us a type-system for Beam records that is independent of any specific programming-language type. There might be multiple Java classes that all have the same schema (for example a Protocol-Buffer class or a POJO class), and Beam will allow us to seamlessly convert between these types.
Schemas also provide a simple way to reason about types across different programming-language APIs.
For more information, see the [programming guide section on Schemas](/documentation/programming-guide/#what-is-a-schema).

{{< language-switcher java >}}

## Using Joins

Beam supports equijoins on schema `PCollections` of Schemas where the join condition depends on the equality of a subset of fields. 

Consider using [`Join`](https://beam.apache.org/releases/javadoc/2.21.0/org/apache/beam/sdk/schemas/transforms/Join.html) if you have multiple collections that provide information about related things, and their structure is known.

For example let's say we have two different collections with user data: one collection contains names and email addresses; the other collection contains names and phone numbers.
We can join the two collections using the name as a common key and the other data as the associated values.
After the join, we have one collection that contains all the information (email address and phone numbers) associated with each name.

The following conceptual example uses two input collections to show the mechanism of [`Join`](https://beam.apache.org/releases/javadoc/2.21.0/org/apache/beam/sdk/schemas/transforms/Join.html).

First, we define Schemas and User data. 

{{< highlight java >}}
{{< code_sample "examples/java/src/test/java/org/apache/beam/examples/snippets/SnippetsTest.java" SchemaJoinPatternCreate >}}
{{< /highlight >}}


Then we create the `Pcollections` for user data and perform join on the two `PCollections` using a [`Join`](https://beam.apache.org/releases/javadoc/2.21.0/org/apache/beam/sdk/schemas/transforms/Join.html). 

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" SchemaJoinPatternJoin >}}
{{< /highlight >}}

The result `Row` is of the type `Row: [Row(emailSchema), Row(phoneSchema)]`, and it can be converted to desired format as shown in the code snippet below.

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" SchemaJoinPatternFormat >}}
{{< /highlight >}}

