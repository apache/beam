import uuid
from typing import Iterable
from typing import Mapping

from yaml import SafeLoader


class SafeLineLoader(SafeLoader):
  """A yaml loader that attaches line information to mappings and strings."""
  class TaggedString(str):
    """A string class to which we can attach metadata.

    This is primarily used to trace a string's origin back to its place in a
    yaml file.
    """
    def __reduce__(self):
      # Pickle as an ordinary string.
      return str, (str(self), )

  def construct_scalar(self, node):
    value = super().construct_scalar(node)
    if isinstance(value, str):
      value = SafeLineLoader.TaggedString(value)
      value._line_ = node.start_mark.line + 1
    return value

  def construct_mapping(self, node, deep=False):
    mapping = super().construct_mapping(node, deep=deep)
    mapping['__line__'] = node.start_mark.line + 1
    mapping['__uuid__'] = self.create_uuid()
    return mapping

  @classmethod
  def create_uuid(cls):
    return str(uuid.uuid4())

  @classmethod
  def strip_metadata(cls, spec, tagged_str=True):
    if isinstance(spec, Mapping):
      return {
          cls.strip_metadata(key, tagged_str):
          cls.strip_metadata(value, tagged_str)
          for (key, value) in spec.items()
          if key not in ('__line__', '__uuid__')
      }
    elif isinstance(spec, Iterable) and not isinstance(spec, (str, bytes)):
      return [cls.strip_metadata(value, tagged_str) for value in spec]
    elif isinstance(spec, SafeLineLoader.TaggedString) and tagged_str:
      return str(spec)
    else:
      return spec

  @staticmethod
  def get_line(obj):
    if isinstance(obj, dict):
      return obj.get('__line__', 'unknown')
    else:
      return getattr(obj, '_line_', 'unknown')
