---
title: "Enrichment with CloudSQL"
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

# Use CloudSQL to enrich data

{{< localstorage language language-py >}}

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.transforms.enrichment_handlers.cloudsql" class="CloudSQLEnrichmentHandler" >}}
      </a>
   </td>
  </tr>
</table>

In Apache Beam <version> and later versions, the enrichment transform includes
a built-in enrichment handler for
[CloudSQL](https://cloud.google.com/sql/docs).
The following example demonstrates how to create a pipeline that use the enrichment transform with the [`CloudSQLEnrichmentHandler`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.cloudsql.html#apache_beam.transforms.enrichment_handlers.cloudsql.CloudSQLEnrichmentHandler) handler.

The data in the CloudSQL PostgreSQL products table instance follows this format:

{{< table >}}
| product_id | name | quantity | region_id |
|:----------:|:----:|:--------:|:---------:|
|     1      |  A   |    2     |     3     |
|     2      |  B   |    3     |     1     |
|     3      |  C   |   10     |     4     |
{{< /table >}}


{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_cloudsql >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment_test.py" enrichment_with_cloudsql >}}
{{< /highlight >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms.enrichment_handlers.cloudsql" class="CloudSQLEnrichmentHandler" >}}