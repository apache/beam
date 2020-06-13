---
layout: section
title: "BigQuery patterns"
section_menu: section-menu/documentation.html
permalink: /documentation/patterns/bigqueryio/
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

# Google BigQuery patterns

The samples on this page show you common patterns for use with BigQueryIO.

{{< language-switcher java py >}}

## BigQueryIO deadletter pattern
In production systems, it is useful to implement the deadletter pattern with BigQueryIO outputting any elements which had errors during processing by BigQueryIO into another PCollection for further processing. 
The samples below print the errors, but in a production system they can be sent to a deadletter table for later correction.  

{{< paragraph class="language-java" >}}
When using `STREAMING_INSERTS`  you can use the `WriteResult` object to access a `PCollection` with the `TableRows` that failed to be inserted into BigQuery. 
If you also set the `withExtendedErrorInfo` property , you will be able to access a `PCollection<BigQueryInsertError>` from the `WriteResult`. The `PCollection` will then include a reference to the table, the data row and the `InsertErrors`. Which errors are added to the deadletter queue is determined via the `InsertRetryPolicy`.
{{< /paragraph >}}

{{< paragraph class="language-py" >}}
In the result tuple you can access `FailedRows` to access the failed inserts.
{{< /paragraph >}}

{{< highlight java >}}
{{< code_sample "examples/java/src/main/java/org/apache/beam/examples/snippets/Snippets.java" BigQueryIODeadLetter >}}
{{< /highlight >}}

{{< highlight py >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/snippets.py" BigQueryIODeadLetter >}}
{{< /highlight >}}
