# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Script to fix the links in the staged website.
Finds all internal links which do not have index.html at the end and appends
index.html in the appropriate place (preserving anchors, etc).

Usage:
  From root directory, after running the jekyll build, execute
  'python .jenkins/append_index_html_to_internal_links.py'.

Dependencies:
  beautifulsoup4
  Installable via pip as 'sudo pip install beautifulsoup4' or apt via
  'sudo apt-get install python-beautifulsoup4'.

"""
from __future__ import print_function

import argparse
import fnmatch
import os
import re
from bs4 import BeautifulSoup

try:
    unicode           # pylint: disable=unicode-builtin
except NameError:
    unicode = str

# Original link match. Matches any string which starts with '/' and doesn't
# have a file extension.
linkMatch = r'^\/(.*\.(?!([^\/]+)$))?[^.]*$'

# Regex which matches strings of type /internal/link/#anchor. Breaks into two
# groups for ease of inserting 'index.html'.
anchorMatch1 = r'(.+\/)(#[^\/]+$)'

# Regex which matches strings of type /internal/link#anchor. Breaks into two
# groups for ease of inserting 'index.html'.
anchorMatch2 = r'(.+\/[a-zA-Z0-9]+)(#[^\/]+$)'

parser = argparse.ArgumentParser(description='Fix links in the staged website.')
parser.add_argument('content_dir', help='Generated content directory to fix links in')
args = parser.parse_args()

matches = []
# Recursively walk content directory and find all html files.
for root, dirnames, filenames in os.walk(args.content_dir):
  for filename in fnmatch.filter(filenames, '*.html'):
    # Javadoc does not have the index.html problem, so omit it.
    if 'javadoc' not in root:
      matches.append(os.path.join(root, filename))

print('Matches: ' + str(len(matches)))
# Iterates over each matched file looking for link matches.
for match in matches:
  print('Fixing links in: ' + match)
  mf = open(match)
  soup = BeautifulSoup(mf)

  # Iterates over every <meta> which is used for aliases - redirected links
  for meta in soup.findAll('meta'):
    try:
      content = meta['content']
      alias = content.replace('0; url=', '')
      if re.match(linkMatch, alias) is not None:
        if alias.endswith('/'):
          # /internal/link/
          meta['content'] = content + 'index.html'
        else:
          # /internal/link
          meta['content'] = content + '/index.html'
        mf.close()

        html = unicode(soup).encode('utf-8')
        # Write back to the file.
        with open(match, "wb") as f:
          print('Replacing ' + content + ' with: ' + meta['content'])
          f.write(html)
    except KeyError as e:
      # Some <meta> tags don't have url.
      continue

  # Iterates over every <a>
  for a in soup.findAll('a'):
    try:
      hr = a['href']
      if re.match(linkMatch, hr) is not None:
        if hr.endswith('/'):
          # /internal/link/
          a['href'] = hr + 'index.html'
        elif re.match(anchorMatch1, hr) is not None:
          # /internal/link/#anchor
          mat = re.match(anchorMatch1, hr)
          a['href'] = mat.group(1) + 'index.html' + mat.group(2)
        elif re.match(anchorMatch2, hr) is not None:
          # /internal/link#anchor
          mat = re.match(anchorMatch2, hr)
          a['href'] = mat.group(1) + '/index.html' + mat.group(2)
        else:
          # /internal/link
          a['href'] = hr + '/index.html'
        mf.close()

        html = unicode(soup).encode('utf-8')
        # Write back to the file.
        with open(match, "wb") as f:
          print('Replacing ' + hr + ' with: ' + a['href'])
          f.write(html)
    except KeyError as e:
      # Some <a> tags don't have an href.
      continue
