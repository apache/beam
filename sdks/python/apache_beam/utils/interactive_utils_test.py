#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

"""Tests for apache_beam.utils.interactive_utils."""

from __future__ import absolute_import

import logging
import sys
import unittest

from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.utils.interactive_utils import IPythonLogHandler
from apache_beam.utils.interactive_utils import register_ipython_log_handler

# TODO(BEAM-8288): clean up the work-around of nose tests using Python2 without
# unittest.mock module.
try:
  from unittest.mock import patch
except ImportError:
  from mock import patch


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@unittest.skipIf(
    sys.version_info < (3, 6), 'The tests require at least Python 3.6 to work.')
class InteractiveUtilsTest(unittest.TestCase):
  def setUp(self):
    register_ipython_log_handler()
    self._interactive_root_logger = logging.getLogger(
        'apache_beam.runners.interactive')

  def test_ipython_log_handler_not_double_registered(self):
    register_ipython_log_handler()
    ipython_log_handlers = list(
        filter(
            lambda x: isinstance(x, IPythonLogHandler),
            [handler for handler in self._interactive_root_logger.handlers]))
    self.assertEqual(1, len(ipython_log_handlers))

  @patch('apache_beam.utils.interactive_utils.IPythonLogHandler.emit')
  def test_default_logging_level_is_info(self, mock_emit):
    # By default the logging level of loggers and log handlers are NOTSET. Also,
    # the propagation is default to true for all loggers. In this scenario, all
    # loggings from child loggers will be propagated to the interactive "root"
    # logger which is set to INFO level that gets handled by the sole log
    # handler IPythonLogHandler which is set to NOTSET. The effect will be
    # everything >= info level will be logged through IPython.core.display to
    # all frontends connected to current kernel.
    dummy_logger = logging.getLogger('apache_beam.runners.interactive.dummy1')
    dummy_logger.info('info')
    mock_emit.assert_called_once()
    dummy_logger.debug('debug')
    # Emit is not called, so it's still called once.
    mock_emit.assert_called_once()

  @patch('apache_beam.utils.interactive_utils.IPythonLogHandler.emit')
  def test_child_module_logger_can_override_logging_level(self, mock_emit):
    # When a child logger's logging level is configured to something that is not
    # NOTSET, it takes back the logging control from the interactive "root"
    # logger by not propagating anything.
    dummy_logger = logging.getLogger('apache_beam.runners.interactive.dummy2')
    dummy_logger.setLevel(logging.DEBUG)
    mock_emit.assert_not_called()
    dummy_logger.debug('debug')
    # Because the dummy child logger is configured to log at DEBUG level, it
    # now propagates DEBUG loggings to the interactive "root" logger.
    mock_emit.assert_called_once()
    # When the dummy child logger is configured to log at CRITICAL level, it
    # will only propagate CRITICAL loggings to the interactive "root" logger.
    dummy_logger.setLevel(logging.CRITICAL)
    # Error loggings will not be handled now.
    dummy_logger.error('error')
    # Emit is not called, so it's still called once.
    mock_emit.assert_called_once()


if __name__ == '__main__':
  unittest.main()
