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
| Cloud SQL (PostgreSQL, MySQL, SQLServer)                    | [Enrichment with CloudSQL](/documentation/transforms/python/elementwise/enrichment-cloudsql/#example)                                                                        |
| Vertex AI Feature Store            | [Enrichment with Vertex AI Feature Store](/documentation/transforms/python/elementwise/enrichment-vertexai/#example-1-enrichment-with-vertex-ai-feature-store)               |
| Vertex AI Feature Store (Legacy)   | [Enrichment with Legacy Vertex AI Feature Store](/documentation/transforms/python/elementwise/enrichment-vertexai/#example-2-enrichment-with-vertex-ai-feature-store-legacy) |
{{< /table >}}

## BigQuery Support

The enrichment transform supports integration with **BigQuery** to dynamically enrich data using BigQuery datasets. By leveraging BigQuery as an external data source, users can execute efficient lookups for data enrichment directly in their Apache Beam pipelines.

To use BigQuery for enrichment:
- Configure your BigQuery table as the data source for the enrichment process.
- Ensure your pipeline has the appropriate credentials and permissions to access the BigQuery dataset.
- Specify the query to extract the data to be used for enrichment.

This integration is particularly beneficial for use cases that require augmenting real-time streaming data with information stored in BigQuery.

---

## Batching

To optimize requests to external services, the enrichment transform uses batching. Instead of performing a lookup for each individual element, the transform groups multiple elements into a batch and performs a single lookup for the entire batch.

### Advantages of Batching:
- **Improved Throughput**: Reduces the number of network calls.
- **Lower Latency**: Fewer round trips to the external service.
- **Cost Optimization**: Minimizes API call costs when working with paid external services.

Users can configure the batch size by specifying parameters in their pipeline setup. Adjusting the batch size can help fine-tune the balance between throughput and latency.

---

## Caching with `with_redis_cache`

For frequently used enrichment data, caching can significantly improve performance by reducing repeated calls to the remote service. Apache Beam's [`with_redis_cache`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment.html#apache_beam.transforms.enrichment.Enrichment.with_redis_cache) method allows you to integrate a Redis cache into the enrichment pipeline.

### Benefits of Caching:
- **Reduced Latency**: Fetches enrichment data from the cache instead of making network calls.
- **Improved Resilience**: Minimizes the impact of network outages or service downtimes.
- **Scalability**: Handles large volumes of enrichment requests efficiently.

To enable caching:
1. Set up a Redis instance accessible by your pipeline.
2. Use the `with_redis_cache` method to configure the cache in your enrichment transform.
3. Specify the time-to-live (TTL) for cache entries to ensure data freshness.

Example:
```python
from apache_beam.transforms.enrichment import Enrichment

# Enrichment pipeline with Redis cache
enriched_data = (input_data
                 | 'Enrich with Cache' >> Enrichment(my_enrichment_transform).with_redis_cache(host, port))


## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms.enrichment" class="Enrichment" >}}