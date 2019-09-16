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

"""Tests for datastore helper."""
from __future__ import absolute_import

import unittest

import mock

# Protect against environments where apitools library is not available.
try:
  from apache_beam.io.gcp.datastore.v1new import helper
  from apache_beam.testing.test_utils import patch_retry
  from google.api_core import exceptions
# TODO(BEAM-4543): Remove TypeError once googledatastore dependency is removed.
except (ImportError, TypeError):
  helper = None


@unittest.skipIf(helper is None, 'GCP dependencies are not installed')
class HelperTest(unittest.TestCase):

  def setUp(self):
    self._mock_datastore = mock.MagicMock()
    patch_retry(self, helper)

  def test_write_mutations_no_errors(self):
    mock_batch = mock.MagicMock()
    mock_throttler = mock.MagicMock()
    rpc_stats_callback = mock.MagicMock()
    mock_throttler.throttle_request.return_value = []
    helper.write_mutations(mock_batch, mock_throttler, rpc_stats_callback)
    rpc_stats_callback.assert_has_calls([
        mock.call(successes=1),
    ])

  def test_write_mutations_throttle_delay_retryable_error(self):
    mock_batch = mock.MagicMock()
    mock_batch.commit.side_effect = [exceptions.DeadlineExceeded('retryable'),
                                     None]
    mock_throttler = mock.MagicMock()
    rpc_stats_callback = mock.MagicMock()
    # First try: throttle once [True, False]
    # Second try: no throttle [False]
    mock_throttler.throttle_request.side_effect = [True, False, False]
    helper.write_mutations(mock_batch, mock_throttler, rpc_stats_callback,
                           throttle_delay=0)
    rpc_stats_callback.assert_has_calls([
        mock.call(successes=1),
        mock.call(throttled_secs=mock.ANY),
        mock.call(errors=1),
    ], any_order=True)
    self.assertEqual(3, rpc_stats_callback.call_count)

  def test_write_mutations_non_retryable_error(self):
    mock_batch = mock.MagicMock()
    mock_batch.commit.side_effect = [
        exceptions.InvalidArgument('non-retryable'),
    ]
    mock_throttler = mock.MagicMock()
    rpc_stats_callback = mock.MagicMock()
    mock_throttler.throttle_request.return_value = False
    with self.assertRaises(exceptions.InvalidArgument):
      helper.write_mutations(mock_batch, mock_throttler, rpc_stats_callback,
                             throttle_delay=0)
    rpc_stats_callback.assert_called_once_with(errors=1)


if __name__ == '__main__':
  unittest.main()
