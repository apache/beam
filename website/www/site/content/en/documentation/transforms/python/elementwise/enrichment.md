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
      {{< button-pydoc path="apache_beam.transforms.enrichment" class="Enrichment" >}}
      </a>
   </td>
  </tr>
</table>


The enrichment transform lets you dynamically enrich data in a pipeline by doing a key-value lookup to a remote service. The transform uses [`RequestResponeIO`](https://beam.apache.org/releases/pydoc/current/apache_beam.io.requestresponseio.html#apache_beam.io.requestresponseio.RequestResponseIO) internally. This feature uses client-side throttling to ensure that the remote service isn't overloaded with requests. If service-side errors occur, like `TooManyRequests` and `Timeout` exceptions, it retries the requests by using exponential backoff.

This transform is available in Apache Beam 2.54.0 and later versions.

## Examples

The following examples demonstrate how to create a pipeline that use the enrichment transform to enrich data from external services.

{{< table >}}
| Service                            | Example                                                                                                                                                                      |
|:-----------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Cloud Bigtable                     | [Enrichment with Bigtable](/documentation/transforms/python/elementwise/enrichment-bigtable/#example)                                                                        |
| Vertex AI Feature Store            | [Enrichment with Vertex AI Feature Store](/documentation/transforms/python/elementwise/enrichment-vertexai/#example-1-enrichment-with-vertex-ai-feature-store)               |
| Vertex AI Feature Store (Legacy)   | [Enrichment with Legacy Vertex AI Feature Store](/documentation/transforms/python/elementwise/enrichment-vertexai/#example-2-enrichment-with-vertex-ai-feature-store-legacy) |
{{< /table >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms.enrichment" class="Enrichment" >}}