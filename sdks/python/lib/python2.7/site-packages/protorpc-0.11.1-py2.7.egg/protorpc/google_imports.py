"""Dynamically decide from where to import other SDK modules.

All other protorpc code should import other SDK modules from
this module. If necessary, add new imports here (in both places).
"""

__author__ = 'yey@google.com (Ye Yuan)'

# pylint: disable=g-import-not-at-top
# pylint: disable=unused-import

try:
  from google.net.proto import ProtocolBuffer
except ImportError:
  pass
