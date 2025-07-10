---
title: "Enrichment with Vertex AI Feature Store"
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

# Enrichment with Google Cloud Vertex AI Feature Store

{{< localstorage language language-py >}}

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store" class="VertexAIFeatureStoreEnrichmentHandler" >}}
      </a>
   </td>
  </tr>
</table>


In Apache Beam 2.55.0 and later versions, the enrichment transform includes a built-in enrichment handler for [Vertex AI Feature Store](https://cloud.google.com/vertex-ai/docs/featurestore).
The following example demonstrates how to create a pipeline that use the enrichment transform with the [`VertexAIFeatureStoreEnrichmentHandler`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.html#apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.VertexAIFeatureStoreEnrichmentHandler) handler and the [`VertexAIFeatureStoreLegacyEnrichmentHandler`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.html#apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.VertexAIFeatureStoreLegacyEnrichmentHandler) handler.

## Example 1: Enrichment with Vertex AI Feature Store

The precomputed feature values stored in Vertex AI Feature Store uses the following format:

{{< table >}}
| user_id  | age  | gender  | state | country |
|:--------:|:----:|:-------:|:-----:|:-------:|
|  21422   |  12  |    0    |   0   |    0    |
|   2963   |  12  |    1    |   1   |    1    |
|  20592   |  12  |    1    |   2   |    2    |
|  76538   |  12  |    1    |   3   |    0    |
{{< /table >}}


{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_vertex_ai >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment_test.py" enrichment_with_vertex_ai >}}
{{< /highlight >}}

## Example 2: Enrichment with Vertex AI Feature Store (legacy)

The precomputed feature values stored in Vertex AI Feature Store (Legacy) use the following format:

{{< table >}}
| entity_id | title                    | genres  |
|:----------|:-------------------------|:--------|
| movie_01  | The Shawshank Redemption | Drama   |
| movie_02  | The Shining              | Horror  |
| movie_04  | The Dark Knight          | Action  |
{{< /table >}}

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_vertex_ai_legacy >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment_test.py" enrichment_with_vertex_ai_legacy >}}
{{< /highlight >}}


## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store" class="VertexAIFeatureStoreEnrichmentHandler" >}}