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

from typing import Any
from typing import Optional

import apache_beam as beam
from apache_beam.yaml import options
from apache_beam.yaml.yaml_errors import maybe_with_exception_handling_transform_fn

try:
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.file import FileEnrichmentHandler
except ImportError:
  Enrichment = None  # type: ignore
  FileEnrichmentHandler = None  # type: ignore

try:
  from apache_beam.transforms.enrichment_handlers.bigquery import BigQueryEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store import VertexAIFeatureStoreEnrichmentHandler
except ImportError:
  BigQueryEnrichmentHandler = None  # type: ignore
  BigTableEnrichmentHandler = None  # type: ignore
  VertexAIFeatureStoreEnrichmentHandler = None  # type: ignore

try:
  from apache_beam.transforms.enrichment_handlers.feast_feature_store import FeastFeatureStoreEnrichmentHandler
except ImportError:
  FeastFeatureStoreEnrichmentHandler = None  # type: ignore


@beam.ptransform.ptransform_fn
@maybe_with_exception_handling_transform_fn
def enrichment_transform(
    pcoll,
    enrichment_handler: str,
    handler_config: dict[str, Any],
    timeout: Optional[float] = 30,
    on_miss: Optional[str] = 'null',
    prefix: Optional[str] = None):
  # pylint: disable=line-too-long

  """
    The Enrichment transform allows one to dynamically enhance elements in a
    pipeline by performing key-value lookups against external services like
    APIs or databases.

    Example using BigTable: ::

        - type: Enrichment
          config:
            enrichment_handler: 'BigTable'
            handler_config:
              project_id: 'apache-beam-testing'
              instance_id: 'beam-test'
              table_id: 'bigtable-enrichment-test'
              row_key: 'product_id'
            timeout: 30

    For more information on Enrichment, see the [Beam docs](
    https://beam.apache.org/documentation/transforms/python/elementwise/enrichment/).

    Args:
        enrichment_handler (str): Specifies the source from where data needs
          to be extracted into the pipeline for enriching data. One of
          "File", "BigQuery", "BigTable", "FeastFeatureStore" or
          "VertexAIFeatureStore".
        handler_config (str): Specifies the parameters for the respective
          enrichment_handler in a YAML/JSON format. To see the full set of
          handler_config parameters, see their corresponding doc pages:

            - [FileEnrichmentHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.file.html#apache_beam.transforms.enrichment_handlers.file.FileEnrichmentHandler)
            - [BigQueryEnrichmentHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.bigquery.html#apache_beam.transforms.enrichment_handlers.bigquery.BigQueryEnrichmentHandler)
            - [BigTableEnrichmentHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.bigtable.html#apache_beam.transforms.enrichment_handlers.bigtable.BigTableEnrichmentHandler)
            - [FeastFeatureStoreEnrichmentHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.feast_feature_store.html#apache_beam.transforms.enrichment_handlers.feast_feature_store.FeastFeatureStoreEnrichmentHandler)
            - [VertexAIFeatureStoreEnrichmentHandler](https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.html#apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store.VertexAIFeatureStoreEnrichmentHandler)
        timeout (float): Timeout for source requests in seconds. Defaults to 30
          seconds.
        on_miss (str): Behavior when a lookup key is not found: `"null"`
          (left join, default), `"drop"` (inner join), `"dlq"` (route to
          `error_handling` output), or `"fail"` (raise error).
        prefix (str): Optional string prefix prepended to enriched field names
          to avoid column collisions.
    """
  options.YamlOptions.check_enabled(pcoll.pipeline, 'Enrichment')

  if not Enrichment:
    raise ValueError(
        f"gcp dependencies not installed. Cannot use {enrichment_handler} "
        f"handler. Please install using 'pip install apache-beam[gcp]'.")

  if (enrichment_handler == 'FeastFeatureStore' and
      not FeastFeatureStoreEnrichmentHandler):
    raise ValueError(
        "FeastFeatureStore handler requires 'feast' package to be installed. " +
        "Please install using 'pip install feast[gcp]' and try again.")

  handler_map = {
      'File': FileEnrichmentHandler,
      'BigQuery': BigQueryEnrichmentHandler,
      'BigTable': BigTableEnrichmentHandler,
      'FeastFeatureStore': FeastFeatureStoreEnrichmentHandler,
      'VertexAIFeatureStore': VertexAIFeatureStoreEnrichmentHandler
  }

  if enrichment_handler not in handler_map:
    raise ValueError(f"Unknown enrichment source: {enrichment_handler}")

  if handler_map[enrichment_handler] is None:
    raise ValueError(
        f"gcp dependencies not installed. Cannot use {enrichment_handler} "
        f"handler. Please install using 'pip install apache-beam[gcp]'.")

  handler = handler_map[enrichment_handler](**handler_config)
  return pcoll | Enrichment(
      source_handler=handler, timeout=timeout, on_miss=on_miss, prefix=prefix)
