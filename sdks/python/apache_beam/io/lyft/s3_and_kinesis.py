import json
import logging
import random
import string

from apache_beam import PTransform
from apache_beam.pvalue import PBegin
from apache_beam.pvalue import PCollection
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.window import GlobalWindows


class EventConfig(object):
    """
    Configuration of analytic event.
    """
    DEFAULT_MAX_OUT_OF_ORDERNESS_MILLIS = 5_000  # maximum allowed delay in time before an element is ignored

    def __init__(self, name):
        self.name = name                          # name of the analytics event.
        self.max_out_of_orderness_millis = EventConfig.DEFAULT_MAX_OUT_OF_ORDERNESS_MILLIS
        self.lookback_days = None                 # historical start time to consume events from.

    def with_max_out_of_orderness_millis(self, out_of_orderness_millis):
        """
        The interval between the maximum timestamp seen so far and the watermark that
        is emitted. For example, if this is set to 1000ms, after seeing a record for
        10:00:01 we will emit a watermark for 10:00:00, indicating that we believe that all
        data from before that time has arrived.
        """
        self.max_out_of_orderness_millis = out_of_orderness_millis
        return self

    def with_lookback_in_days(self, lookback_days):
        self.lookback_days = lookback_days
        return self


class S3Config(object):
    """
    S3 configuration.
    """
    DEFAULT_S3_PARALLELISM = 1              # parallelism for s3 source connector. Defaults to 1.
    DEFAULT_LOOKBACK_THRESHOLD_HOURS = 23   # threshold in hours for consuming events from S3.

    def __init__(self):
        self.parallelism = S3Config.DEFAULT_S3_PARALLELISM
        self.lookback_threshold_hours = S3Config.DEFAULT_LOOKBACK_THRESHOLD_HOURS

    def with_parallelism(self, parallelism):
        self.parallelism = parallelism
        return self

    def with_lookback_threshold_hours(self, lookback_threshold_hours):
        self.lookback_threshold_hours = lookback_threshold_hours
        return self


class S3AndKinesisInput(PTransform):
    """Custom composite transform that uses Kinesis and S3 as
    input sources. This wraps the streamingplatform-dryft-sdk SourceConnector.
    Only works with the portable Flink runner.
    """
    DEFAULT_KINESIS_PARALLELISM = -1
    DEFAULT_START_MODE = 'TRIM_HORIZON'

    def __init__(self):
        super().__init__()
        self.events_config = []
        self.s3_config = S3Config()
        self.source_name = 'S3andKinesis_' + self._get_random_source_name()
        self.kinesis_properties = {'aws.region': 'us-east-1'}
        self.stream_start_mode = S3AndKinesisInput.DEFAULT_START_MODE
        self.kinesis_parallelism = S3AndKinesisInput.DEFAULT_KINESIS_PARALLELISM
        self.app_env = "production"

    def expand(self, pbegin):
        assert isinstance(pbegin, PBegin), (
                'Input to transform must be a PBegin but found %s' % pbegin)
        return PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
        return Windowing(GlobalWindows())

    def infer_output_type(self, unused_input_type):
        return bytes

    def with_app_environment(self, app_env):
        """
        Add the application environment.
        Will be used to connect to the appropriate S3 buckets
        staging or prod.
        Defaults to production
        """
        self.app_env = app_env
        return self

    def with_event_config(self, event_config):
        """
        Append EventConfig to the list of event configuration.
        :param event_config: An instance of EventConfig
        :return: S3AndKinesisInput
        """
        self.events_config.append(event_config)
        return self

    def with_kinesis_stream_name(self, stream_name):
        self.stream_name = stream_name
        return self

    # Defaults to -1
    def with_kinesis_parallelism(self, parallelism):
        self.kinesis_parallelism = parallelism
        return self

    # Defaults to TRIM_HORIZON
    def with_kinesis_stream_start_mode(self, stream_start_mode):
        self.stream_start_mode = stream_start_mode
        return self

    def with_kinesis_property(self, key, value):
        self.kinesis_properties[key] = value
        return self

    def with_s3_config(self, s3_config):
        self.s3_config = s3_config
        return self

    def with_source_name(self, source_name):
        self.source_name = source_name
        return self

    def with_kinesis_endpoint(self, endpoint, access_key, secret_key):
        self.kinesis_properties.pop('aws.region', None)
        self.kinesis_properties['aws.endpoint'] = endpoint
        self.kinesis_properties['aws.credentials.provider.basic.accesskeyid'] = access_key
        self.kinesis_properties['aws.credentials.provider.basic.secretkey'] = secret_key
        return self

    @staticmethod
    @PTransform.register_urn("lyft:flinkS3AndKinesisInput", None)
    def from_runner_api_parameter(_unused_ptransform, spec_parameter, _unused_context):
        logging.info("S3AndKinesisInput spec: %s", spec_parameter)
        instance = S3AndKinesisInput()
        payload = json.loads(spec_parameter)
        instance.source_name = payload['source_name']
        instance.app_env = payload['app_env']
        s3_config_dict = payload['s3']

        lookback_threshold_hours=s3_config_dict.get('lookback_threshold_hours', S3Config.DEFAULT_LOOKBACK_THRESHOLD_HOURS)
        s3_parallelism=s3_config_dict.get('parallelism', S3Config.DEFAULT_S3_PARALLELISM)
        s3_config = S3Config()
        s3_config.with_lookback_threshold_hours(lookback_threshold_hours)
        s3_config.with_parallelism(s3_parallelism)

        instance.s3_config = s3_config

        kinesis_config_dict = payload['kinesis']
        instance.stream_name = kinesis_config_dict.get('stream')
        instance.kinesis_properties = kinesis_config_dict.get('properties', None)
        instance.kinesis_parallelism = kinesis_config_dict.get('parallelism',
                                                               S3AndKinesisInput.DEFAULT_KINESIS_PARALLELISM)
        instance.stream_start_mode = kinesis_config_dict.get('stream_start_mode',
                                                             S3AndKinesisInput.DEFAULT_START_MODE)
        events_list = payload['events']
        instance.events_config = []
        for event in events_list:
            assert event.get('name') is not None, "Event name must be set"
            event_config = EventConfig(event.get('name'))
            max_out_of_orderness_millis = event.get('max_out_of_orderness_millis',
                                                    EventConfig.DEFAULT_MAX_OUT_OF_ORDERNESS_MILLIS)
            event_config.with_max_out_of_orderness_millis(max_out_of_orderness_millis)

            lookback_days = event.get('lookback_days', None)
            if lookback_days is not None:
                event_config.with_lookback_in_days(lookback_days)
            instance.events_config.append(event_config)

        return instance

    def to_runner_api_parameter(self, _unused_context):
        assert isinstance(self, S3AndKinesisInput), \
            "expected instance of S3AndKinesisInput, but got %s" % self.__class__
        assert self.stream_name is not None, "Kinesis stream name not set"

        json_map = {
            'source_name': self.source_name,
            'app_env': self.app_env,
            'kinesis': {
                'stream': self.stream_name,
                'properties': self.kinesis_properties,
                'parallelism': self.kinesis_parallelism,
                'stream_start_mode': self.stream_start_mode
            },
            's3': {
                'parallelism': self.s3_config.parallelism,
                'lookback_threshold_hours': self.s3_config.lookback_threshold_hours
            },
        }

        event_list_json = []
        for event_config in self.events_config:
            event_map = {
                'name': event_config.name,
                'max_out_of_orderness_millis': event_config.max_out_of_orderness_millis,
                'lookback_days': event_config.lookback_days
            }
            event_list_json.append(event_map)

        json_map['events'] = event_list_json

        return "lyft:flinkS3AndKinesisInput", json.dumps(json_map, default=self._set_to_list_conversion)

    def _set_to_list_conversion(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return obj

    def _get_random_source_name(self):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for _ in range(4))
