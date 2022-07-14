from ........utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class ExpansionMethods(object):

  class Enum(object):
    JAVA_CLASS_LOOKUP = PropertiesFromEnumValue('beam:expansion:payload:java_class_lookup:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

