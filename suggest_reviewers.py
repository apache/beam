#!/bin/bash
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

from __future__ import print_function

import argparse
import collections
import fnmatch
import logging
import requests
import random
import string

CODEOWNERS_FILE = 'CODEOWNERS_'


class Codeowners(object):

  def __init__(self, codeowners_file):
    self.path_owner_map = collections.OrderedDict()
    for line in open(codeowners_file):
      line = line.split('#', 1)[0].strip()
      if not line:
        continue
      path, owners = line.split(' ', 1)
      self.path_owner_map[path] = owners.split()

  def _get_reviewers_for_path(self, path):
    res = []
    for path_pattern, owners in self.path_owner_map.iteritems():
      if '*' not in path_pattern:
        path_pattern += '*'
        if not path_pattern.startswith('/'):
          path_pattern = '*/' + path_pattern
      if fnmatch.fnmatch(path, path_pattern):
        res = (path_pattern, owners)
    return res

  def get_reviewers(self, paths, random_seed):
    random.seed(random_seed)
    reviewers = set()
    for path in paths:
      path_pattern, candidates = self._get_reviewers_for_path(path)
      if not candidates:
        logging.warning('No candidate reviewers for path: %s', path)
        continue
      selected_candidate = candidates[random.randint(0, len(candidates) - 1)]
      logging.info('Selected reviewer %s for: %s (path_pattern: %s)',
                   selected_candidate, path, path_pattern)
      reviewers.add(selected_candidate)

    return reviewers


class PullRequest(object):

  def __init__(self, number):
    self.number = number
    url = 'https://api.github.com/repos/apache/beam/pulls/%d/files' % number
    self.files = []
    for file_json in requests.get(url).json():
      self.files.append('/' + file_json['filename'])


def main():
  logging.getLogger().setLevel(logging.INFO)
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--pr',
      type=int,
      required=True,
      help='Github pull request number to suggest reviewers for.')
  args = parser.parse_args()

  codeowners = Codeowners(CODEOWNERS_FILE)
  pr = PullRequest(args.pr)
  reviewers = codeowners.get_reviewers(pr.files, random_seed=pr.number)
  if reviewers:
    print('Suggested reviewers: %s' % ', '.join(reviewers))
  else:
    print('No suggested reviewers. :(')


if __name__ == '__main__':
  main()
