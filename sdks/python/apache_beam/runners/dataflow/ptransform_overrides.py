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

"""Ptransform overrides for DataflowRunner."""

from __future__ import absolute_import

from apache_beam.coders import typecoders
from apache_beam.pipeline import PTransformOverride


class CreatePTransformOverride(PTransformOverride):
  """A ``PTransformOverride`` for ``Create`` in streaming mode."""

  def matches(self, applied_ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam import Create
    from apache_beam.options.pipeline_options import StandardOptions

    if isinstance(applied_ptransform.transform, Create):
      standard_options = (applied_ptransform
                          .outputs[None]
                          .pipeline._options
                          .view_as(StandardOptions))
      return standard_options.streaming
    else:
      return False

  def get_replacement_transform(self, ptransform):
    # Imported here to avoid circular dependencies.
    # pylint: disable=wrong-import-order, wrong-import-position
    from apache_beam.runners.dataflow.native_io.streaming_create import \
      StreamingCreate
    coder = typecoders.registry.get_coder(ptransform.get_output_type())
    return StreamingCreate(ptransform.values, coder)
