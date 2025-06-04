"""Customizations to how Python code objects are pickled.

This module provides functions to pickle code objects, especially lambdas, in a
consistent way. It addresses issues with non-deterministic pickling by creating
stable references to code objects using their qualified names and bytecode
hashes.
"""

import os
import sys


def get_relative_path(path):
  """Returns the path of filename relative to the first directory in sys.path
  contained in filename. Returns the unchanged filename if it is not in any
  sys.path directory.

  Args:
    path: The path to the file.
  """
  for dir_path in sys.path:
    # The path for /aaa/bbb/c.py is relative to /aaa/bbb and not /aaa/bb.
    if not dir_path.endswith(os.path.sep):
      dir_path += os.path.sep
    if path.startswith(dir_path):
      return os.path.relpath(path, dir_path)
  return path


def get_normalized_path(path):
  """Returns a normalized path. This function is intended to be overridden."""
  # Use relative paths to make pickling lambdas deterministic for google3
  # This is needed only for code running inside Google on borg.
  if '/borglet/' in path:
    return get_relative_path(path)

  return path
