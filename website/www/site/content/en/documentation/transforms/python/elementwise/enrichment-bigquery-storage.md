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
Row(sale_id=1001, product_id=101, customer_id=501, quantity=2, product_name='Laptop Pro', category='Electronics', unit_price=999.99)
Row(sale_id=1002, product_id=102, customer_id=502, quantity=1, product_name='Wireless Mouse', category='Electronics', unit_price=29.99)
Row(sale_id=1003, product_id=103, customer_id=503, quantity=5, product_name='Office Chair', category='Furniture', unit_price=199.99)
{{< /highlight >}}

## Batched Processing for Large Datasets

For improved performance with large datasets, you can enable batching:

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigquery_storage_batched >}}
{{</ highlight >}}

## Column Aliasing

Rename columns in the output for cleaner field names:

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigquery_storage_column_aliasing >}}
{{</ highlight >}}

{{< paragraph class="notebook-skip" >}}
Output:
{{< /paragraph >}}
{{< highlight class="notebook-skip" >}}
Row(transaction_id='TXN-001', customer_id='C123', amount=150.0, customer_name='John Smith', contact_email='john.smith@email.com', membership_level='Premium', member_since='2023-01-15')
{{< /highlight >}}

## Multiple Field Matching

Use multiple fields for complex matching scenarios:

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigquery_storage_multiple_fields >}}
{{</ highlight >}}

## Custom Filtering Logic

Use custom functions for advanced filtering requirements:

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigquery_storage_custom_function >}}
{{</ highlight >}}

## Performance-Optimized Configuration

For high-throughput scenarios, optimize performance with parallel stream processing:

{{< highlight language="py" >}}
{{< code_sample "sdks/python/apache_beam/examples/snippets/transforms/elementwise/enrichment.py" enrichment_with_bigquery_storage_performance_tuned >}}
{{</ highlight >}}

## Related transforms

Not applicable.

{{< button-pydoc path="apache_beam.transforms.enrichment_handlers.bigquery_storage_read" class="BigQueryStorageEnrichmentHandler" >}}
