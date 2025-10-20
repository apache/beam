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

"""Factory to create grpc channel."""
# pytype: skip-file

import grpc


class GRPCChannelFactory(grpc.StreamStreamClientInterceptor):
  DEFAULT_OPTIONS = [
      # Default: 30000ms (30s), increased to 180s to reduce ping frequency
      ("grpc.keepalive_time_ms", 180000),
      # Default: 5000ms (5s), increased to 10 minutes for stability
      ("grpc.keepalive_timeout_ms", 600000),
      # Default: 2, set to 0 to allow unlimited pings without data
      ("grpc.http2.max_pings_without_data", 0),
      # Default: False, set to True to allow keepalive pings when no calls
      ("grpc.keepalive_permit_without_calls", True),
      # Default: 300000ms (5min), increased to 10min for stability
      ("grpc.http2.min_recv_ping_interval_without_data_ms", 600000),
      # Default: 300000ms (5min), increased to 120s for conservative pings
      ("grpc.http2.min_sent_ping_interval_without_data_ms", 120000),
      # Default: 2, set to 0 to allow unlimited ping strikes
      ("grpc.http2.max_ping_strikes", 0),
      # Default: 0 (disabled), enable socket reuse for better handling
      ("grpc.so_reuseport", 1),
      # Default: 0 (disabled), set 30s TCP timeout for connection control
      ("grpc.tcp_user_timeout_ms", 30000),
  ]

  def __init__(self):
    pass

  @staticmethod
  def insecure_channel(target, options=None):
    if options is None:
      options = []
    return grpc.insecure_channel(
        target, options=options + GRPCChannelFactory.DEFAULT_OPTIONS)

  @staticmethod
  def secure_channel(target, credentials, options=None):
    if options is None:
      options = []
    return grpc.secure_channel(
        target,
        credentials,
        options=options + GRPCChannelFactory.DEFAULT_OPTIONS)
