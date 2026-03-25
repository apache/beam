from ........utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class Annotations(object):

  class Enum(object):
    CONFIG_ROW_KEY = PropertiesFromEnumValue('', 'config_row', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    CONFIG_ROW_SCHEMA_KEY = PropertiesFromEnumValue('', 'config_row_schema', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SCHEMATRANSFORM_URN_KEY = PropertiesFromEnumValue('', 'schematransform_urn', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MANAGED_UNDERLYING_TRANSFORM_URN_KEY = PropertiesFromEnumValue('', 'managed_underlying_transform_urn', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class ExpansionMethods(object):

  class Enum(object):
    JAVA_CLASS_LOOKUP = PropertiesFromEnumValue('beam:expansion:payload:java_class_lookup:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SCHEMA_TRANSFORM = PropertiesFromEnumValue('beam:expansion:payload:schematransform:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class ManagedTransforms(object):

  class Urns(object):
    ICEBERG_READ = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:iceberg_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ICEBERG_WRITE = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:iceberg_write:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    KAFKA_READ = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:kafka_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    KAFKA_WRITE = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:kafka_write:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    BIGQUERY_READ = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:bigquery_storage_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    BIGQUERY_WRITE = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:bigquery_write:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ICEBERG_CDC_READ = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:iceberg_cdc_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    POSTGRES_READ = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:postgres_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    POSTGRES_WRITE = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:postgres_write:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MYSQL_READ = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:mysql_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MYSQL_WRITE = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:mysql_write:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SQL_SERVER_READ = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:sql_server_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SQL_SERVER_WRITE = PropertiesFromEnumValue('beam:schematransform:org.apache.beam:sql_server_write:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

