---
title: "Enrichment with Bigtable"
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

# Use Enrichment with Bigtable

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.transforms" class="Enrichment" >}}
      </a>
   </td>
  </tr>
</table>

The following examples demonstrate how to create pipelines that use the Enrichment transform and BigTableEnrichmentHandler.

## Example: Enrich data with Bigtable

In this example, we create a pipeline that uses a BigTableEnrichmentHandler with Enrichment transform.

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigtable >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment_test.py" enrichment_with_bigtable >}}
{{< /highlight >}}