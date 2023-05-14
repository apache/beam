"""Library of supported test cases.

All tests should conform to the NotebookTestCase abstract class.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from abc import ABC, abstractmethod
import re


class NotebookTestCase(ABC):
  @abstractmethod
  def __init__(self, setup):
    """Construct a NotebookTestCase.

    Args:
      setup: arbitrary JSON-serializable object specified by test spec
    """
    pass

  # should raise exception on failure
  @abstractmethod
  def check(self, cell):
    """Check correctness against single Jupyter cell.

    Args:
      cell: JSON representation of single cell.

    Returns None if test succeeds, raise exception if test fails.
    """
    pass


################################################################################


class RegexMatch(NotebookTestCase):
  """Checks if given string exists anywhere in the cell."""
  def __init__(self, setup):
    self.regex = re.compile(setup)

  def check(self, cell):
    if not self.regex.search(str(cell)):
      raise Exception(
          "Could not find {} in {}".format(self.regex.pattern, cell))
