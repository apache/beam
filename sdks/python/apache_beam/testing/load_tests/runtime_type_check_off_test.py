import logging

from .runtime_type_check.runtime_type_check_test import BaseRunTimeTypeCheckTest


class RunTimeTypeCheckOffTest(BaseRunTimeTypeCheckTest):
  def __init__(self):
    self.runtime_type_check = False
    super(RunTimeTypeCheckOffTest, self).__init__()


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  RunTimeTypeCheckOffTest().run()
