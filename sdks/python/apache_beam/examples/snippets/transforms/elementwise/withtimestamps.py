# coding=utf-8
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


def time_tuple2unix_time():
  # [START time_tuple2unix_time]
  import time

  time_tuple = time.strptime('2020-03-19 20:50:00', '%Y-%m-%d %H:%M:%S')
  unix_time = time.mktime(time_tuple)
  # [END time_tuple2unix_time]
  return unix_time


def datetime2unix_time():
  # [START datetime2unix_time]
  import time
  import datetime

  now = datetime.datetime.now()
  time_tuple = now.timetuple()
  unix_time = time.mktime(time_tuple)
  # [END datetime2unix_time]
  return unix_time
