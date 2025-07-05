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

"""This module is intended for internal use only. Nothing defined here provides
any backwards-compatibility guarantee.
"""

from uuid import uuid4


# Registry of typings for a schema by UUID
class SchemaTypeRegistry(object):
  def __init__(self):
    self.by_id = {}
    self.by_typing = {}

  def generate_new_id(self):
    for _ in range(100):
      schema_id = str(uuid4())
      if schema_id not in self.by_id:
        return schema_id

    raise AssertionError(
        "Failed to generate a unique UUID for schema after "
        f"100 tries! Registry contains {len(self.by_id)} "
        "schemas.")

  def add(self, typing, schema):
    if schema.id:
      self.by_id[schema.id] = (typing, schema)

  def get_typing_by_id(self, unique_id):
    if not unique_id:
      return None
    result = self.by_id.get(unique_id, None)
    return result[0] if result is not None else None

  def get_schema_by_id(self, unique_id):
    if not unique_id:
      return None
    result = self.by_id.get(unique_id, None)
    return result[1] if result is not None else None


SCHEMA_REGISTRY = SchemaTypeRegistry()
