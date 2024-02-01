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

# Enrichment transform for data enrichment

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

Enrichment transform enable users to dynamically enrich their data within a pipeline by doing a key-value lookup to the remote service. It uses [RequestResponeIO](https://beam.apache.org/releases/pydoc/current/apache_beam.io.requestresponseio.html#apache_beam.io.requestresponseio.RequestResponseIO) internally that make sures that the remote service is not overloaded with requests by doing a client-side throttling whenever required. Additionally, in case of service-side errors like TooManyRequests and Timeout exceptions, it retries the requests with exponential backoff.

Starting Apache Beam 2.54.0, the package provides a built-in enrichment handler for [Cloud Bigtable](https://cloud.google.com/bigtable?hl=en).

## Examples

The following examples show how to create pipelines that use the Enrichment transform for data enrichment with built-in enrichment handlers.

{{< table >}}
| Service | Example |
| ----- | ----- |
| Bigtable | [BigtableEnrichmentHandler](/documentation/transforms/python/elementwise/enrichment-bigtable/#example-enrich-data-with-bigtable) |:
{{< /table >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms" class="Enrichment" >}}