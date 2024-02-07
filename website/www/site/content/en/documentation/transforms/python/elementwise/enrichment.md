---
title: "Enrichment"
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

# Enrichment transform

{{< localstorage language language-py >}}

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.transforms" class="Enrichment" >}}
      </a>
   </td>
  </tr>
</table>

The enrichment transform lets you dynamically enrich their data within a pipeline by doing a key-value lookup to the remote service. It uses [RequestResponeIO](https://beam.apache.org/releases/pydoc/current/apache_beam.io.requestresponseio.html#apache_beam.io.requestresponseio.RequestResponseIO) internally that make sures that the remote service is not overloaded with requests by doing a client-side throttling whenever required. Additionally, in case of service-side errors like TooManyRequests and Timeout exceptions, it retries the requests with exponential backoff.

Starting Apache Beam 2.54.0, the package provides a built-in enrichment handler for [Bigtable](https://cloud.google.com/bigtable?hl=en).

## Use Bigtable to enrich data

The following example demonstrates how to create pipelines that use the enrichment transform with BigTableEnrichmentHandler.

The data stored in the Bigtable cluster uses the following format:

|  Row key  |  product:product_id  |  product:product_name  |  product:product_stock  |
|:---------:|:--------------------:|:----------------------:|:-----------------------:|
|     1     |          1           |        pixel 5         |            2            |
|     2     |          2           |        pixel 6         |            4            |
|     3     |          3           |        pixel 7         |           20            |
|     4     |          4           |        pixel 8         |           10            |


{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigtable >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment_test.py" enrichment_with_bigtable >}}
{{< /highlight >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms" class="Enrichment" >}}