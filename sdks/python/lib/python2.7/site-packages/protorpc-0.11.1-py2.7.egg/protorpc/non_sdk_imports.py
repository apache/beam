"""Dynamically decide from where to import other non SDK Google modules.

All other protorpc code should import other non SDK modules from
this module. If necessary, add new imports here (in both places).
"""

__author__ = 'yey@google.com (Ye Yuan)'

# pylint: disable=g-import-not-at-top
# pylint: disable=unused-import

try:
  from google.protobuf import descriptor
  normal_environment = True
except ImportError:
  normal_environment = False

if normal_environment:
  from google.protobuf import descriptor_pb2
  from google.protobuf import message
  from google.protobuf import reflection
