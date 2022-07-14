from ........utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class MonitoringInfo(object):

  class MonitoringInfoLabels(object):
    TRANSFORM = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='PTRANSFORM'))
    PCOLLECTION = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='PCOLLECTION'))
    WINDOWING_STRATEGY = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='WINDOWING_STRATEGY'))
    CODER = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='CODER'))
    ENVIRONMENT = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='ENVIRONMENT'))
    NAMESPACE = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='NAMESPACE'))
    NAME = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='NAME'))
    SERVICE = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='SERVICE'))
    METHOD = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='METHOD'))
    RESOURCE = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='RESOURCE'))
    STATUS = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='STATUS'))
    BIGQUERY_PROJECT_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='BIGQUERY_PROJECT_ID'))
    BIGQUERY_DATASET = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='BIGQUERY_DATASET'))
    BIGQUERY_TABLE = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='BIGQUERY_TABLE'))
    BIGQUERY_VIEW = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='BIGQUERY_VIEW'))
    BIGQUERY_QUERY_NAME = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='BIGQUERY_QUERY_NAME'))
    GCS_BUCKET = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='GCS_BUCKET'))
    GCS_PROJECT_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='GCS_PROJECT_ID'))
    DATASTORE_PROJECT = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='DATASTORE_PROJECT'))
    DATASTORE_NAMESPACE = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='DATASTORE_NAMESPACE'))
    BIGTABLE_PROJECT_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='BIGTABLE_PROJECT_ID'))
    INSTANCE_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='INSTANCE_ID'))
    TABLE_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='TABLE_ID'))
    SPANNER_PROJECT_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='SPANNER_PROJECT_ID'))
    SPANNER_DATABASE_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='SPANNER_DATABASE_ID'))
    SPANNER_TABLE_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='SPANNER_TABLE_ID'))
    SPANNER_INSTANCE_ID = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='SPANNER_INSTANCE_ID'))
    SPANNER_QUERY_NAME = PropertiesFromEnumValue('', '', EMPTY_MONITORING_INFO_SPEC, metrics_pb2.MonitoringInfoLabelProps(name='SPANNER_QUERY_NAME'))


class MonitoringInfoSpecs(object):

  class Enum(object):
    USER_SUM_INT64 = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:sum_int64:v1', type='beam:metrics:sum_int64:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_SUM_DOUBLE = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:sum_double:v1', type='beam:metrics:sum_double:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_DISTRIBUTION_INT64 = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:distribution_int64:v1', type='beam:metrics:distribution_int64:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_DISTRIBUTION_DOUBLE = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:distribution_double:v1', type='beam:metrics:distribution_double:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_LATEST_INT64 = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:latest_int64:v1', type='beam:metrics:latest_int64:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_LATEST_DOUBLE = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:latest_double:v1', type='beam:metrics:latest_double:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_TOP_N_INT64 = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:top_n_int64:v1', type='beam:metrics:top_n_int64:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_TOP_N_DOUBLE = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:top_n_double:v1', type='beam:metrics:top_n_double:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_BOTTOM_N_INT64 = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:bottom_n_int64:v1', type='beam:metrics:bottom_n_int64:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    USER_BOTTOM_N_DOUBLE = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:user:bottom_n_double:v1', type='beam:metrics:bottom_n_double:v1', required_labels=['PTRANSFORM', 'NAMESPACE', 'NAME'], annotations=[metrics_pb2.Annotation(key='description', value='URN utilized to report user metric.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    ELEMENT_COUNT = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:element_count:v1', type='beam:metrics:sum_int64:v1', required_labels=['PCOLLECTION'], annotations=[metrics_pb2.Annotation(key='description', value='The total elements output to a Pcollection by a PTransform.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    SAMPLED_BYTE_SIZE = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:sampled_byte_size:v1', type='beam:metrics:distribution_int64:v1', required_labels=['PCOLLECTION'], annotations=[metrics_pb2.Annotation(key='description', value='The total byte size and count of a sampled  set (or all) of elements in the pcollection. Sampling is used  because calculating the byte count involves serializing the  elements which is CPU intensive.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    START_BUNDLE_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:pardo_execution_time:start_bundle_msecs:v1', type='beam:metrics:sum_int64:v1', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the start bundlefunction in a pardo')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS_BUNDLE_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:pardo_execution_time:process_bundle_msecs:v1', type='beam:metrics:sum_int64:v1', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the process bundlefunction in a pardo')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    FINISH_BUNDLE_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:pardo_execution_time:finish_bundle_msecs:v1', type='beam:metrics:sum_int64:v1', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the finish bundle function in a pardo')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    TOTAL_MSECS = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:ptransform_execution_time:total_msecs:v1', type='beam:metrics:sum_int64:v1', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The total estimated execution time of the ptransform')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    WORK_REMAINING = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:ptransform_progress:remaining:v1', type='beam:metrics:progress:v1', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The remaining amount of work for each active element. Each active element represents an independent amount of work not shared with any other active element.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    WORK_COMPLETED = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:ptransform_progress:completed:v1', type='beam:metrics:progress:v1', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The remaining amount of work for each active element. Each active element represents an independent amount of work not shared with any other active element.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    DATA_CHANNEL_READ_INDEX = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:data_channel:read_index:v1', type='beam:metrics:sum_int64:v1', required_labels=['PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='The read index of the data channel.')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    API_REQUEST_COUNT = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:io:api_request_count:v1', type='beam:metrics:sum_int64:v1', required_labels=['SERVICE', 'METHOD', 'RESOURCE', 'PTRANSFORM', 'STATUS'], annotations=[metrics_pb2.Annotation(key='description', value='Request counts with status made to IO service APIs to batch read or write elements.'), metrics_pb2.Annotation(key='process_metric', value='true')]), EMPTY_MONITORING_INFO_LABEL_PROPS)
    API_REQUEST_LATENCIES = PropertiesFromEnumValue('', '', metrics_pb2.MonitoringInfoSpec(urn='beam:metric:io:api_request_latencies:v1', type='beam:metrics:histogram_int64:v1', required_labels=['SERVICE', 'METHOD', 'RESOURCE', 'PTRANSFORM'], annotations=[metrics_pb2.Annotation(key='description', value='Histogram counts for request latencies made to IO service APIs to batch read or write elements.'), metrics_pb2.Annotation(key='units', value='Milliseconds'), metrics_pb2.Annotation(key='process_metric', value='true')]), EMPTY_MONITORING_INFO_LABEL_PROPS)


class MonitoringInfoTypeUrns(object):

  class Enum(object):
    SUM_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:sum_int64:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SUM_DOUBLE_TYPE = PropertiesFromEnumValue('beam:metrics:sum_double:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DISTRIBUTION_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:distribution_int64:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DISTRIBUTION_DOUBLE_TYPE = PropertiesFromEnumValue('beam:metrics:distribution_double:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    LATEST_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:latest_int64:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    LATEST_DOUBLE_TYPE = PropertiesFromEnumValue('beam:metrics:latest_double:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TOP_N_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:top_n_int64:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TOP_N_DOUBLE_TYPE = PropertiesFromEnumValue('beam:metrics:top_n_double:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    BOTTOM_N_INT64_TYPE = PropertiesFromEnumValue('beam:metrics:bottom_n_int64:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    BOTTOM_N_DOUBLE_TYPE = PropertiesFromEnumValue('beam:metrics:bottom_n_double:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROGRESS_TYPE = PropertiesFromEnumValue('beam:metrics:progress:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

