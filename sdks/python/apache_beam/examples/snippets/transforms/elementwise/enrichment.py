# coding=utf-8
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file
# pylint: disable=line-too-long


def enrichment_with_bigtable():
  # [START enrichment_with_bigtable]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler

  project_id = 'apache-beam-testing'
  instance_id = 'beam-test'
  table_id = 'bigtable-enrichment-test'
  row_key = 'product_id'

  data = [
      beam.Row(sale_id=1, customer_id=1, product_id=1, quantity=1),
      beam.Row(sale_id=3, customer_id=3, product_id=2, quantity=3),
      beam.Row(sale_id=5, customer_id=5, product_id=4, quantity=2)
  ]

  bigtable_handler = BigTableEnrichmentHandler(
      project_id=project_id,
      instance_id=instance_id,
      table_id=table_id,
      row_key=row_key)
  with beam.Pipeline() as p:
    _ = (
        p
        | "Create" >> beam.Create(data)
        | "Enrich W/ BigTable" >> Enrichment(bigtable_handler)
        | "Print" >> beam.Map(print))
  # [END enrichment_with_bigtable]


def enrichment_with_vertex_ai():
  # [START enrichment_with_vertex_ai]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store \
    import VertexAIFeatureStoreEnrichmentHandler

  project_id = 'apache-beam-testing'
  location = 'us-central1'
  api_endpoint = f"{location}-aiplatform.googleapis.com"
  data = [
      beam.Row(user_id='2963', product_id=14235, sale_price=15.0),
      beam.Row(user_id='21422', product_id=11203, sale_price=12.0),
      beam.Row(user_id='20592', product_id=8579, sale_price=9.0),
  ]

  vertex_ai_handler = VertexAIFeatureStoreEnrichmentHandler(
      project=project_id,
      location=location,
      api_endpoint=api_endpoint,
      feature_store_name="vertexai_enrichment_example",
      feature_view_name="users",
      row_key="user_id",
  )
  with beam.Pipeline() as p:
    _ = (
        p
        | "Create" >> beam.Create(data)
        | "Enrich W/ Vertex AI" >> Enrichment(vertex_ai_handler)
        | "Print" >> beam.Map(print))
  # [END enrichment_with_vertex_ai]


def enrichment_with_vertex_ai_legacy():
  # [START enrichment_with_vertex_ai_legacy]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store \
    import VertexAIFeatureStoreLegacyEnrichmentHandler

  project_id = 'apache-beam-testing'
  location = 'us-central1'
  api_endpoint = f"{location}-aiplatform.googleapis.com"
  data = [
      beam.Row(entity_id="movie_01", title='The Shawshank Redemption'),
      beam.Row(entity_id="movie_02", title="The Shining"),
      beam.Row(entity_id="movie_04", title='The Dark Knight'),
  ]

  vertex_ai_handler = VertexAIFeatureStoreLegacyEnrichmentHandler(
      project=project_id,
      location=location,
      api_endpoint=api_endpoint,
      entity_type_id='movies',
      feature_store_id="movie_prediction_unique",
      feature_ids=["title", "genres"],
      row_key="entity_id",
  )
  with beam.Pipeline() as p:
    _ = (
        p
        | "Create" >> beam.Create(data)
        | "Enrich W/ Vertex AI" >> Enrichment(vertex_ai_handler)
        | "Print" >> beam.Map(print))
  # [END enrichment_with_vertex_ai_legacy]


def enrichment_with_bigquery_storage_basic():
  # [START enrichment_with_bigquery_storage_basic]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery_storage_read import BigQueryStorageEnrichmentHandler

  # Sample sales data to enrich
  sales_data = [
      beam.Row(sale_id=1001, product_id=101, customer_id=501, quantity=2),
      beam.Row(sale_id=1002, product_id=102, customer_id=502, quantity=1),
      beam.Row(sale_id=1003, product_id=103, customer_id=503, quantity=5),
  ]

  # Basic enrichment - enrich sales data with product information
  handler = BigQueryStorageEnrichmentHandler(
      project='your-gcp-project',
      table_name='your-project.ecommerce.products',
      row_restriction_template=
      'id = {product_id}',  # BQ column 'id' matches input 'product_id'
      fields=['product_id'],
      column_names=[
          'id as product_id', 'product_name', 'category', 'unit_price'
      ],
  )

  with beam.Pipeline() as p:
    _ = (
        p
        | "Create Sales Data" >> beam.Create(sales_data)
        | "Enrich with Product Info" >> Enrichment(handler)
        | "Print Results" >> beam.Map(print))
  # [END enrichment_with_bigquery_storage_basic]


def enrichment_with_bigquery_storage_batched():
  # [START enrichment_with_bigquery_storage_batched]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery_storage_read import BigQueryStorageEnrichmentHandler

  # Large dataset for batch processing
  order_data = [
      beam.Row(
          order_id=f"ORD-{i}", customer_id=f"CUST-{i%100}", region_id=i % 10)
      for i in range(1, 1001)  # 1000 orders
  ]

  # Batched enrichment for better performance with large datasets
  handler = BigQueryStorageEnrichmentHandler(
      project='your-gcp-project',
      table_name='your-project.locations.regions',
      row_restriction_template=
      'region_code = {region_id}',  # BQ column 'region_code' matches input 'region_id'
      fields=['region_id'],
      column_names=[
          'region_code as region_id', 'region_name', 'country', 'timezone'
      ],
      min_batch_size=50,  # Process at least 50 elements together
      max_batch_size=200,  # Maximum 200 elements per batch
      max_batch_duration_secs=10,  # Maximum 10 seconds wait time
  )

  with beam.Pipeline() as p:
    _ = (
        p
        | "Create Order Data" >> beam.Create(order_data)
        | "Enrich with Region Info" >> Enrichment(handler)
        | "Count Results" >> beam.combiners.Count.Globally()
        | "Print Count" >> beam.Map(print))
  # [END enrichment_with_bigquery_storage_batched]


def enrichment_with_bigquery_storage_column_aliasing():
  # [START enrichment_with_bigquery_storage_column_aliasing]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery_storage_read import BigQueryStorageEnrichmentHandler

  # Customer data to enrich with profile information
  customer_transactions = [
      beam.Row(transaction_id="TXN-001", customer_id="C123", amount=150.00),
      beam.Row(transaction_id="TXN-002", customer_id="C124", amount=75.50),
      beam.Row(transaction_id="TXN-003", customer_id="C125", amount=200.00),
  ]

  # Column aliasing for cleaner output field names
  handler = BigQueryStorageEnrichmentHandler(
      project='your-gcp-project',
      table_name='your-project.customers.profiles',
      row_restriction_template=
      'cust_id = "{customer_id}"',  # BQ column 'cust_id' matches input 'customer_id'
      fields=['customer_id'],
      column_names=[
          'cust_id as customer_id',  # Ensure field matching
          'full_name as customer_name',
          'email_address as contact_email',
          'account_type as membership_level',
          'created_date as member_since'
      ],
  )

  with beam.Pipeline() as p:
    _ = (
        p
        | "Create Transaction Data" >> beam.Create(customer_transactions)
        | "Enrich with Customer Profile" >> Enrichment(handler)
        | "Print Results" >> beam.Map(print))
  # [END enrichment_with_bigquery_storage_column_aliasing]


def enrichment_with_bigquery_storage_multiple_fields():
  # [START enrichment_with_bigquery_storage_multiple_fields]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery_storage_read import BigQueryStorageEnrichmentHandler

  # Inventory data with composite keys
  inventory_requests = [
      beam.Row(product_id="PROD-001", warehouse_id="WH-NY", date="2024-01-15"),
      beam.Row(product_id="PROD-002", warehouse_id="WH-CA", date="2024-01-15"),
      beam.Row(product_id="PROD-001", warehouse_id="WH-TX", date="2024-01-15"),
  ]

  # Multi-field matching for complex queries
  handler = BigQueryStorageEnrichmentHandler(
      project='your-gcp-project',
      table_name='your-project.inventory.stock_levels',
      row_restriction_template=
      'prod_id = "{product_id}" AND wh_id = "{warehouse_id}" AND stock_date = "{date}"',
      fields=['product_id', 'warehouse_id', 'date'],
      column_names=[
          'prod_id as product_id',  # BQ column 'prod_id' matches input 'product_id'
          'wh_id as warehouse_id',  # BQ column 'wh_id' matches input 'warehouse_id'
          'stock_date as date',  # BQ column 'stock_date' matches input 'date'
          'current_stock',
          'reserved_stock',
          'available_stock',
          'last_updated'
      ],
  )

  with beam.Pipeline() as p:
    _ = (
        p
        | "Create Inventory Requests" >> beam.Create(inventory_requests)
        | "Enrich with Stock Levels" >> Enrichment(handler)
        | "Print Results" >> beam.Map(print))
  # [END enrichment_with_bigquery_storage_multiple_fields]


def enrichment_with_bigquery_storage_custom_function():
  # [START enrichment_with_bigquery_storage_custom_function]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery_storage_read import BigQueryStorageEnrichmentHandler

  # Analytics data requiring custom filtering logic
  user_events = [
      beam.Row(
          user_id="U123",
          event_type="purchase",
          session_id="S456",
          timestamp="2024-01-15T10:30:00"),
      beam.Row(
          user_id="U124",
          event_type="view",
          session_id="S457",
          timestamp="2024-01-15T11:15:00"),
      beam.Row(
          user_id="U125",
          event_type="purchase",
          session_id="S458",
          timestamp="2024-01-15T12:00:00"),
  ]

  def build_user_filter(condition_values, primary_keys, req_row):
    """Custom function to build complex filter conditions"""
    user_id = condition_values.get('user_id')
    event_type = condition_values.get('event_type')

    # Build filter based on event type - note BQ uses 'uid' column
    if event_type == 'purchase':
      return f'uid = "{user_id}" AND is_premium_user = true'
    else:
      return f'uid = "{user_id}"'

  def extract_condition_values(req_row):
    """Extract values needed for filtering"""
    return {'user_id': req_row.user_id, 'event_type': req_row.event_type}

  # Custom filtering with condition_value_fn and row_restriction_template_fn
  handler = BigQueryStorageEnrichmentHandler(
      project='your-gcp-project',
      table_name='your-project.users.profiles',
      row_restriction_template_fn=build_user_filter,
      condition_value_fn=extract_condition_values,
      column_names=[
          'uid as user_id',  # BQ column 'uid' matches input 'user_id'
          'user_name',
          'subscription_tier',
          'last_login',
          'total_purchases'
      ],
  )

  with beam.Pipeline() as p:
    _ = (
        p
        | "Create User Events" >> beam.Create(user_events)
        | "Enrich with User Profiles" >> Enrichment(handler)
        | "Print Results" >> beam.Map(print))
  # [END enrichment_with_bigquery_storage_custom_function]


def enrichment_with_bigquery_storage_performance_tuned():
  # [START enrichment_with_bigquery_storage_performance_tuned]
  import apache_beam as beam
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery_storage_read import BigQueryStorageEnrichmentHandler

  # Large-scale data processing scenario
  transaction_data = [
      beam.Row(
          account_id=f"ACC-{i}",
          transaction_date="2024-01-15",
          amount=float(i * 10.5))
      for i in range(1, 10001)  # 10,000 transactions
  ]

  # Performance-optimized configuration
  handler = BigQueryStorageEnrichmentHandler(
      project='your-gcp-project',
      table_name='your-project.accounts.account_details',
      row_restriction_template=
      'acc_id = "{account_id}"',  # BQ column 'acc_id' matches input 'account_id'
      fields=['account_id'],
      column_names=[
          'acc_id as account_id',  # BQ column 'acc_id' matches input 'account_id'
          'account_holder',
          'account_type',
          'branch_code',
          'opening_balance'
      ],
      min_batch_size=100,  # Large batches for efficiency
      max_batch_size=1000,  # Very large maximum batch size
      max_batch_duration_secs=5,  # Quick batching
      max_parallel_streams=8,  # Parallel stream processing
      max_stream_count=50,  # Optimize BigQuery Storage streams
  )

  with beam.Pipeline() as p:
    _ = (
        p
        | "Create Transaction Data" >> beam.Create(transaction_data)
        | "Enrich with Account Details" >> Enrichment(handler)
        | "Count by Account Type" >> beam.Map(lambda x: (x.account_type, 1))
        | "Group by Account Type" >> beam.GroupByKey()
        | "Sum Counts" >> beam.Map(lambda x: (x[0], sum(x[1])))
        | "Print Summary" >> beam.Map(print))
  # [END enrichment_with_bigquery_storage_performance_tuned]
