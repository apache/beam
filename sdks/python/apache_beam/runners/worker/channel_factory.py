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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import grpc


class GRPCChannelFactory(grpc.StreamStreamClientInterceptor):
  DEFAULT_OPTIONS = [("grpc.keepalive_time_ms", 20000)]

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
        target, credentials,
        options=options + GRPCChannelFactory.DEFAULT_OPTIONS)
