import json
import logging
import random
import string
from collections import namedtuple

from apache_beam import PTransform
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.window import GlobalWindows

Event = namedtuple('Event', 'name lateness_in_sec lookback_days')
S3Config = namedtuple('S3Config', 'parallelism lookback_hours')


class S3Input(PTransform):
  """Custom composite transform that uses S3 as
  input sources. This wraps the streamingplatform-dryft-sdk SourceConnector.
  Only works with the portable Flink runner.
  """

  def __init__(self):
    super().__init__()
    self.events = []
    self.s3_config = S3Config(None, None)
    self.source_name = 'S3_' + self._get_random_source_name()
    self.app_env = "production"

  def expand(self, pbegin):
    assert isinstance(pbegin, PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    return PCollection(pbegin.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def with_event(self, event):
    self.events.append(event)
    return self

  def with_s3_config(self, s3_config):
    self.s3_config = s3_config
    return self

  def with_source_name(self, source_name):
    self.source_name = source_name
    return self

  def with_app_environment(self, app_env):
    """
    Add the application environment.
    Will be used to connect to the appropriate S3 buckets
    staging or prod.
    Defaults to production
    """
    self.app_env = app_env
    return self

  @staticmethod
  @PTransform.register_urn("lyft:flinkS3Input", None)
  def from_runner_api_parameter(_unused_ptransform, spec_parameter, _unused_context):
    logging.info("S3Input spec: %s", spec_parameter)
    instance = S3Input()
    payload = json.loads(spec_parameter)
    instance.source_name = payload['source_name']
    instance.app_env = payload['app_env']
    s3_config_dict = payload['s3']
    instance.s3_config = S3Config(
        parallelism=s3_config_dict.get('parallelism', None),
        lookback_hours=s3_config_dict.get('lookback_hours', None)
    )

    events_list = payload['events']
    instance.events = []
    for event in events_list:
      assert event.get('name') is not None, "Event name must be set"
      instance.events.append(
          Event(
              name=event.get('name'),
              lateness_in_sec=event.get('lateness_in_sec', None),
              lookback_days=event.get('lookback_days', None)
          )
      )
    return instance

  def to_runner_api_parameter(self, _unused_context):
    assert isinstance(self, S3Input), \
      "expected instance of S3Input, but got %s" % self.__class__

    json_map = {
      'source_name': self.source_name,
      'app_env': self.app_env,
      's3': {
        'parallelism': self.s3_config.parallelism,
        'lookback_hours': self.s3_config.lookback_hours
      },
    }

    event_list_json = []
    for e in self.events:
      assert isinstance(e, Event), "expected instance of Event, but got %s" % type(e)
      event_map = {'name': e.name, 'lateness_in_sec': e.lateness_in_sec, 'lookback_days': e.lookback_days}
      event_list_json.append(event_map)

    json_map['events'] = event_list_json

    return "lyft:flinkS3Input", json.dumps(json_map, default=self._set_to_list_conversion)

  def _set_to_list_conversion(self, obj):
    if isinstance(obj, set):
      return list(obj)
    return obj

  def _get_random_source_name(self):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(4))
