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

"""Apache Beam SDK version information and utilities."""


import re


__version__ = '0.3.0-incubating.dev'  # TODO: PEP 440 and incubating suffix


# The following utilities are legacy code from the Maven integration;
# see BEAM-378 for further details.


# Reads the actual version from pom.xml file,
def get_version_from_pom():
  with open('pom.xml', 'r') as f:
    pom = f.read()
    regex = (r'.*<parent>\s*'
             r'<groupId>[a-z\.]+</groupId>\s*'
             r'<artifactId>[a-z\-]+</artifactId>\s*'
             r'<version>([0-9a-zA-Z\.\-]+)</version>.*')
    pattern = re.compile(str(regex))
    search = pattern.search(pom)
    version = search.group(1)
    version = version.replace("-SNAPSHOT", ".dev")
    # TODO: PEP 440 and incubating suffix
    return version


# Synchronizes apache_beam.__version__ field for later usage
def sync_version(version):
  init_path = 'apache_beam/__init__.py'
  regex = r'^__version__\s*=\s*".*"'
  with open(init_path, "r") as f:
    lines = f.readlines()
  with open(init_path, "w") as f:
    for line in lines:
      if re.search(regex, line):
        f.write(re.sub(regex, '__version__ = "%s"' % version, line))
      else:
        f.write(line)
