from apache_beam.utils.proto_utils import pack_Any, unpack_Any, to_Timestamp
import unittest

from google.protobuf import timestamp_pb2


class ProtoUtilsTest(unittest.TestCase):
  def make_proto_timestamp(self):
    # type: () -> timestamp_pb2.Timestamp
    return to_Timestamp(0)

  def test_none_pack(self):
    packed_none = pack_Any(None)
    assert packed_none is None

  def test_date_pack(self):
    # type: () -> None
    proto_timestamp = self.make_proto_timestamp()
    packed_msg = pack_Any(proto_timestamp)
    orig_msg = unpack_Any(packed_msg, timestamp_pb2.Timestamp)
    none_msg = unpack_Any(packed_msg, None)
    assert proto_timestamp == orig_msg
    assert none_msg is None
