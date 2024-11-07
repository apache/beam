import json
import logging

from apache_beam import pvalue
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.core import Windowing


class FlinkKinesisInput(PTransform):
  """Custom transform that wraps a Flink Kinesis consumer - only works with the
  portable Flink runner."""

  LYFT_BASE64_ZLIB_JSON_ENCODING = "lyft-base64-zlib-json"

  consumer_properties = {
    'aws.region': 'us-east-1',
    'flink.stream.initpos': 'TRIM_HORIZON'
  }
  stream = None
  encoding = None
  max_out_of_orderness_millis = None
  use_watermark_tracker = None

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    return pvalue.PCollection(pbegin.pipeline)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, _unused_context):
    assert isinstance(self, FlinkKinesisInput), \
      "expected instance of FlinkKinesisInput, but got %s" % self.__class__
    assert self.stream is not None, "topic not set"
    assert len(self.consumer_properties) > 0, "consumer properties not set"

    return ("lyft:flinkKinesisInput", json.dumps({
      'stream': self.stream,
      'encoding': self.encoding,
      'max_out_of_orderness_millis': self.max_out_of_orderness_millis,
      'properties': self.consumer_properties,
      'use_watermark_tracker': self.use_watermark_tracker}))

  @staticmethod
  @PTransform.register_urn("lyft:flinkKinesisInput", None)
  def from_runner_api_parameter(_unused_ptransform, spec_parameter, _unused_context):
    logging.info("kinesis spec: %s", spec_parameter)
    instance = FlinkKinesisInput()
    payload = json.loads(spec_parameter)
    instance.stream = payload['stream']
    instance.encoding = payload['encoding']
    instance.max_out_of_orderness_millis = payload['max_out_of_orderness_millis']
    instance.consumer_properties = payload['properties']
    instance.use_watermark_tracker = payload['use_watermark_tracker']
    return instance

  def with_stream(self, stream):
    self.stream = stream
    return self

  def set_consumer_property(self, key, value):
    self.consumer_properties[key] = value
    return self

  def with_encoding(self, encoding):
    """
    Sets the encoding used for messages in the stream. This is required for
    watermarks to be emitted. Currently supported encodings:

    * FlinkKinesisInput.LYFT_BASE64_ZLIB_JSON_ENCODING
    """
    self.encoding = encoding
    return self

  def with_max_out_of_orderness_millis(self, max_out_of_orderness_millis):
    """
    The interval between the maximum timestamp seen so far and the watermark that
    is emitted. For example, if this is set to 1000ms, after seeing a record for
    10:00:01 we will emit a watermark for 10:00:00, indicating that we believe that all
    data from before that time has arrived.
    """
    self.max_out_of_orderness_millis = max_out_of_orderness_millis
    return self

  def with_endpoint(self, endpoint, access_key, secret_key):
    # cannot have both region and endpoint
    self.consumer_properties.pop('aws.region', None)
    self.set_consumer_property('aws.endpoint', endpoint)
    self.set_consumer_property('aws.credentials.provider.basic.accesskeyid', access_key)
    self.set_consumer_property('aws.credentials.provider.basic.secretkey', secret_key)
    return self

  def with_watermark_tracker(self, use_watermark_tracker):
    """
    Enables consumer watermark synchronization. This can be enabled to reduce event time skew.
    https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kinesis/#event-time-alignment-for-shard-consumers
    """
    self.use_watermark_tracker = use_watermark_tracker
    return self
