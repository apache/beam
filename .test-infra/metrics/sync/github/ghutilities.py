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

'''This file contains a list of utilities for working with GitHub data.'''

from datetime import datetime
import re

GITHUB_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def datetimeFromGHTimeStr(text):
  '''Parse GitHub time format into datetime structure.'''
  return datetime.strptime(text, GITHUB_DATETIME_FORMAT)


def datetimeToGHTimeStr(timestamp):
  '''Convert datetime to GitHub datetime string'''
  return timestamp.strftime(GITHUB_DATETIME_FORMAT)


def findMentions(text):
  '''Returns all mentions in text. Skips "username".'''
  matches = re.findall("@(\\w+)", text)
  return list(filter(lambda x: (x != "username" and x != ""), matches))
