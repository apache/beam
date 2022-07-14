from ........utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class FixedWindowsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:window_fn:fixed_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class GlobalWindowsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:window_fn:global_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class SessionWindowsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:window_fn:session_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class SlidingWindowsPayload(object):

  class Enum(object):
    PROPERTIES = PropertiesFromEnumValue('beam:window_fn:sliding_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

