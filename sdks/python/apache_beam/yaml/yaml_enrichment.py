from typing import Any, Dict
import apache_beam as beam
from apache_beam.transforms.enrichment_handlers.bigquery import BigQueryEnrichmentHandler
from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
from apache_beam.transforms.enrichment_handlers.feast_feature_store import FeastFeatureStoreEnrichmentHandler
from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store import VertexAIFeatureStoreEnrichmentHandler
from apache_beam.transforms.enrichment import Enrichment
from typing import Optional

@beam.ptransform.ptransform_fn
def enrichment_transform(pcoll, enrichment_handler: str, handler_config: Dict[str, Any], timeout: Optional[float] = 30):
    """
    The Enrichment transform allows you to dynamically enhance elements in a pipeline 
    by performing key-value lookups against external services like APIs or databases. 

    Args:
        enrichment_handler: Specifies the source from where data needs to be extracted
            into the pipeline for enriching data. It can be a string value in ["BigQuery", 
            "BigTable", "FeastFeatureStore", "VertexAIFeatureStore"].
        handler_config: Specifies the parameters for the respective enrichment_handler in a dictionary format. 
            BigQuery:  project, table_name, row_restriction_template, fields, column_names, condition_value_fn, query_fn, min_batch_size, max_batch_size
            BigTable: project_id, instance_id, table_id row_key, row_filter, app_profile_id, encoding, ow_key_fn, exception_level, include_timestamp
            FeastFeatureStore: feature_store_yaml_path, feature_names, feature_service_name, full_feature_names, entity_row_fn, exception_level
            VertexAIFeatureStore: project, location, api_endpoint, feature_store_name:, feature_view_name, row_key, exception_level

    Example Usage:
    
        - type: Enrichment
          config:
            enrichment_handler: 'BigTable'
            handler_config:
                project_id: 'apache-beam-testing'
                instance_id: 'beam-test'
                table_id: 'bigtable-enrichment-test'
                row_key: 'product_id'
            timeout: 30

    """
    if enrichment_handler is None:
        raise ValueError("Missing 'source' in enrichment spec.")
    if handler_config is None:
        raise ValueError("Missing 'handler_config' in enrichment spec.")

    handler_map = {
        'BigQuery': BigQueryEnrichmentHandler,
        'BigTable': BigTableEnrichmentHandler,
        'FeastFeatureStore': FeastFeatureStoreEnrichmentHandler,
        'VertexAIFeatureStore': VertexAIFeatureStoreEnrichmentHandler
    }

    if enrichment_handler not in handler_map:
        raise ValueError(f"Unknown enrichment source: {enrichment_handler}")

    handler = handler_map[enrichment_handler](**handler_config)
    return pcoll | Enrichment(source_handler = handler, timeout = timeout)
