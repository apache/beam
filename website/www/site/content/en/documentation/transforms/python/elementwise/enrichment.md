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

The enrichment transform lets you dynamically enrich data in a pipeline by doing a key-value lookup to a remote service. The transform uses [`RequestResponeIO`](https://beam.apache.org/releases/pydoc/current/apache_beam.io.requestresponseio.html#apache_beam.io.requestresponseio.RequestResponseIO) internally. This feature uses client-side throttling to ensure that the remote service isn't overloaded with requests. If service-side errors occur, like `TooManyRequests` and `Timeout` exceptions, it retries the requests by using exponential backoff.

In Apache Beam 2.54.0 and later versions, the transform includes a built-in enrichment handler for [Cloud Bigtable](https://cloud.google.com/bigtable?hl=en).

## Examples

The following examples show how to create pipelines that use the Enrichment transform for data enrichment with built-in enrichment handlers.

{{< table >}}
| Service | Example |
| ----- | ----- |
| Bigtable | [BigTableEnrichmentHandler](/documentation/transforms/python/elementwise/enrichment-bigtable/#example-enrich-data-with-bigtable) |:
{{< /table >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms" class="Enrichment" >}}