---
title: "Enrichment with BigQuery Storage Read API"
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

## Use BigQuery Storage API to enrich data

{{< localstorage language language-py >}}

<table>
  <tr>
    <td>
      <a>
      {{< button-pydoc path="apache_beam.transforms.enrichment_handlers.bigquery_storage_read" class="BigQueryStorageEnrichmentHandler" >}}
      </a>
   </td>
  </tr>
</table>

In Apache Beam <version> and later versions, the enrichment transform includes a built-in enrichment handler for [BigQuery](https://cloud.google.com/bigquery/docs/overview) using the [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage?hl=en).
The following examples demonstrate how to create pipelines that use the enrichment transform with the [`BigQueryStorageEnrichmentHandler`](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.bigquery_storage_read.html#apache_beam.transforms.enrichment_handlers.bigquery_storage_read.BigQueryStorageEnrichmentHandler) handler, showcasing its flexibility and various use cases.

## Field Matching Requirements

When using BigQuery Storage enrichment, it's important to ensure that field names match between your input data and the enriched output. The `fields` parameter specifies columns from your input data used for matching, while `column_names` specifies which columns to retrieve from BigQuery.

If BigQuery column names differ from your input field names, use aliases in `column_names` (e.g., `'bq_column_name as input_field_name'`) to ensure proper field matching.

## Basic Enrichment Example

This example shows basic product information enrichment for sales data:

{{< table >}}
| sale_id | product_id | customer_id | quantity |
|:-------:|:----------:|:-----------:|:--------:|
|  1001   |    101     |     501     |    2     |
|  1002   |    102     |     502     |    1     |
|  1003   |    103     |     503     |    5     |
{{< /table >}}

Enriched with product table data:

{{< table >}}
| id | product_name   | category    | unit_price |
|:--:|:--------------:|:-----------:|:----------:|
| 101| Laptop Pro     | Electronics |   999.99   |
| 102| Wireless Mouse | Electronics |   29.99    |
| 103| Office Chair   | Furniture   |   199.99   |
{{< /table >}}

Note: The BigQuery table uses `id` as the column name, but our input data has `product_id`. We use `'id as product_id'` in `column_names` to ensure proper field matching.

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigquery_storage_basic >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
Row(sale_id=1001, product_id=101, customer_id=501, quantity=2, product_id=101, product_name='Laptop Pro', category='Electronics', unit_price=999.99)
Row(sale_id=1002, product_id=102, customer_id=502, quantity=1, product_id=102, product_name='Wireless Mouse', category='Electronics', unit_price=29.99)
Row(sale_id=1003, product_id=103, customer_id=503, quantity=5, product_id=103, product_name='Office Chair', category='Furniture', unit_price=199.99)
{{< /highlight >}}

## Advanced: Custom Filtering Logic

This example demonstrates advanced enrichment features including:
- **Conditional enrichment**: Only enrich sales where `quantity > 2` and `category == "Electronics"`
- **Multiple key matching**: Match on both `product_id` and `category` fields
- **Custom field mapping**: Use aliases to rename BigQuery columns in the output (`id as prod_id`, `product_name as name`, etc.)

Input data includes sales with different quantities and categories, but only Electronics products with quantity > 2 will be enriched:

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigquery_storage_custom_function >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
Row(sale_id=1002, product_id=102, category='Electronics', customer_id=502, quantity=4, \
    prod_id=102, name='Wireless Mouse', category='Electronics', price=29.99)
Row(sale_id=1004, product_id=101, category='Electronics', customer_id=504, quantity=6, \
    prod_id=101, name='Laptop Pro', category='Electronics', price=999.99)
{{< /highlight >}}

## FAQ: Advanced Options

**Q: How do I enable batching for large datasets?**

A: Use the `min_batch_size`, `max_batch_size`, and `max_batch_duration_secs` parameters in `BigQueryStorageEnrichmentHandler` to control batch size and timing.

**Q: How do I use custom filtering logic?**

A: Provide a `row_restriction_template_fn` and `condition_value_fn` to the handler. See the advanced example above.

**Q: How do I tune performance for high-throughput scenarios?**

A: Use `max_parallel_streams` and `max_stream_count` in the handler for parallel BigQuery reads. Increase batch sizes for efficiency.

**Q: How do I use column aliasing?**

A: Use the `as` keyword in `column_names` (e.g., `'bq_column as my_field'`) to rename columns in the output.

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms.enrichment_handlers.bigquery_storage_read" class="BigQueryStorageEnrichmentHandler" >}}
