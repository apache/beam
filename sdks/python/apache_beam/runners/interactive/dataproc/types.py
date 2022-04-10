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

from dataclasses import dataclass
from typing import Optional


@dataclass
class MasterURLIdentifier:
  project_id: Optional[str] = None
  region: Optional[str] = None
  cluster_name: Optional[str] = None

  def __key(self):
    return (self.project_id, self.region, self.cluster_name)

  def __hash__(self):
    return hash(self.__key())

  def __eq__(self, other):
    if isinstance(other, MasterURLIdentifier):
      return self.__key() == other.__key()
    raise NotImplementedError(
        'Comparisons are only supported between '
        'instances of MasterURLIdentifier.')
