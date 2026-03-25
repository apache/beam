from ........utils import PropertiesFromEnumValue
from ...pipeline.v1 import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class FnApiTransforms(object):

  class Runner(object):
    DATA_SOURCE = PropertiesFromEnumValue('beam:runner:source:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DATA_SINK = PropertiesFromEnumValue('beam:runner:sink:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

