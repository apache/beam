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
