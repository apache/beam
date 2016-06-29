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

"""Apache Beam SDK version utilities."""

import pkg_resources
from lxml import etree


def get_version_from_pom():
  pom = etree.parse('pom.xml')
  elements = pom.xpath(r'/pom:project/pom:parent/pom:version', namespaces={'pom':'http://maven.apache.org/POM/4.0.0'})
  version = elements[0].text
  version = version.replace("-SNAPSHOT", ".dev")  # PEP 440
  # TODO: PEP 440 and incubating suffix
  return version


def get_distribution_version():
  return pkg_resources.get_distribution("apache_beam").version
