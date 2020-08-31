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


class S3AndKinesisInput(PTransform):
    """Custom composite transform that uses Kinesis and S3 as
    input sources. This wraps the streamingplatform-dryft-sdk SourceConnector.
    Only works with the portable Flink runner.
    """

    def __init__(self):
        super().__init__()
        self.events = []
        self.s3_config = S3Config(None, None)
        self.source_name = 'S3andKinesis_' + self._get_random_source_name()
        self.kinesis_properties = {'aws.region': 'us-east-1'}
        self.stream_start_mode = 'TRIM_HORIZON'
        self.kinesis_parallelism = 1

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

    def with_kinesis_stream_name(self, stream_name):
        self.stream_name = stream_name
        return self

    # Defaults to 1
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
        s3_config_dict = payload['s3']
        instance.s3_config = S3Config(
            parallelism=s3_config_dict.get('parallelism', None),
            lookback_hours=s3_config_dict.get('lookback_hours', None)
        )
        kinesis_config_dict = payload['kinesis']
        instance.stream_name = kinesis_config_dict.get('stream')
        instance.kinesis_properties = kinesis_config_dict.get('properties', None)
        instance.kinesis_parallelism = kinesis_config_dict.get('parallelism', None)
        instance.stream_start_mode = kinesis_config_dict.get('stream_start_mode', None)
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
        assert isinstance(self, S3AndKinesisInput), \
            "expected instance of S3AndKinesisInput, but got %s" % self.__class__
        assert self.stream_name is not None, "Kinesis stream name not set"

        json_map = {
            'source_name': self.source_name,
            'kinesis': {
                'stream': self.stream_name,
                'properties': self.kinesis_properties,
                'parallelism': self.kinesis_parallelism,
                'stream_start_mode': self.stream_start_mode
            },
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

        return "lyft:flinkS3AndKinesisInput", json.dumps(json_map, default=self._set_to_list_conversion)

    def _set_to_list_conversion(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return obj

    def _get_random_source_name(self):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for _ in range(4))
