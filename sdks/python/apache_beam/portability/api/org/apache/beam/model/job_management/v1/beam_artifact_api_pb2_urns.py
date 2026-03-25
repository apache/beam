from ........utils import PropertiesFromEnumValue
from ...pipeline.v1 import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class CommitManifestResponse(object):

  class Constants(object):
    NO_ARTIFACTS_STAGED_TOKEN = PropertiesFromEnumValue('', '__no_artifacts_staged__', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

