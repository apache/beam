from google.protobuf import any_pb2
from google.protobuf import struct_pb2


def pack_Any(msg):
  """Creates a protobuf Any with msg as its content.

  Returns None if msg is None.
  """
  if msg is None:
    return None
  else:
    result = any_pb2.Any()
    result.Pack(msg)
    return result


def unpack_Any(any_msg, msg_class):
  """Unpacks any_msg into msg_class.

  Returns None if msg_class is None.
  """
  if msg_class is None:
    return None
  else:
    msg = msg_class()
    any_msg.Unpack(msg)
    return msg


def pack_Struct(**kwargs):
  """Returns a struct containing the values indicated by kwargs.
  """
  msg = struct_pb2.Struct()
  for key, value in kwargs.items():
    msg[key] = value  # pylint: disable=unsubscriptable-object
  return msg
