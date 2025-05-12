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

"""Module for tracking a chain of beam_sql magics applied.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

import importlib
import logging
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional
from typing import Set
from typing import Union

import apache_beam as beam
from apache_beam.internal import pickler
from apache_beam.runners.interactive.sql.utils import register_coder_for_schema
from apache_beam.runners.interactive.utils import create_var_in_main
from apache_beam.runners.interactive.utils import pcoll_by_name
from apache_beam.runners.interactive.utils import progress_indicated
from apache_beam.transforms.sql import SqlTransform
from apache_beam.utils.interactive_utils import is_in_ipython

_LOGGER = logging.getLogger(__name__)


@dataclass
class SqlNode:
  """Each SqlNode represents a beam_sql magic applied.

  Attributes:
    output_name: the watched unique name of the beam_sql output. Can be used as
      an identifier.
    source: the inputs consumed by this node. Can be a pipeline or a set of
      PCollections represented by their variable names watched. When it's a
      pipeline, the node computes from raw values in the query, so the output
      can be consumed by any SqlNode in any SqlChain.
    query: the SQL query applied by this node.
    schemas: the schemas (NamedTuple classes) used by this node.
    evaluated: the pipelines this node has been evaluated for.
    next: the next SqlNode applied chronologically.
    execution_count: the execution count if in an IPython env.
  """
  output_name: str
  source: Union[beam.Pipeline, Set[str]]
  query: str
  schemas: Set[Any] = None
  evaluated: Set[beam.Pipeline] = None
  next: Optional['SqlNode'] = None
  execution_count: int = 0

  def __post_init__(self):
    if not self.schemas:
      self.schemas = set()
    if not self.evaluated:
      self.evaluated = set()
    if is_in_ipython():
      from IPython import get_ipython
      self.execution_count = get_ipython().execution_count

  def __hash__(self):
    return hash(
        (self.output_name, self.source, self.query, self.execution_count))

  def to_pipeline(self, pipeline: Optional[beam.Pipeline]) -> beam.Pipeline:
    """Converts the chain into an executable pipeline."""
    if pipeline not in self.evaluated:
      # The whole chain should form a single pipeline.
      source = self.source
      if isinstance(self.source, beam.Pipeline):
        if pipeline:  # use the known pipeline
          source = pipeline
        else:  # use the source pipeline
          pipeline = self.source
      else:
        name_to_pcoll = pcoll_by_name()
        if len(self.source) == 1:
          source = name_to_pcoll.get(next(iter(self.source)))
        else:
          source = {s: name_to_pcoll.get(s) for s in self.source}
      if isinstance(source, beam.Pipeline):
        output = source | 'beam_sql_{}_{}'.format(
            self.output_name, self.execution_count) >> SqlTransform(self.query)
      else:
        output = source | 'schema_loaded_beam_sql_{}_{}'.format(
            self.output_name, self.execution_count
        ) >> SchemaLoadedSqlTransform(
            self.output_name, self.query, self.schemas, self.execution_count)
      _ = create_var_in_main(self.output_name, output)
      self.evaluated.add(pipeline)
    if self.next:
      return self.next.to_pipeline(pipeline)
    else:
      return pipeline


class SchemaLoadedSqlTransform(beam.PTransform):
  """PTransform that loads schema before executing SQL.

  When submitting a pipeline to remote runner for execution, schemas defined in
  the main module are not available without save_main_session. However,
  save_main_session might fail when there is anything unpicklable. This DoFn
  makes sure only the schemas needed are pickled locally and restored later on
  workers.
  """
  def __init__(self, output_name, query, schemas, execution_count):
    self.output_name = output_name
    self.query = query
    self.schemas = schemas
    self.execution_count = execution_count
    # TODO(BEAM-8123): clean up this attribute or the whole wrapper PTransform.
    # Dill does not preserve everything. On the other hand, save_main_session
    # is not stable. Until cloudpickle replaces dill in Beam, we work around
    # it by explicitly pickling annotations and load schemas in remote main
    # sessions.
    self.schema_annotations = [s.__annotations__ for s in self.schemas]

  class _SqlTransformDoFn(beam.DoFn):
    """The DoFn yields all its input without any transform but a setup to
    configure the main session."""
    def __init__(self, schemas, annotations):
      self.pickled_schemas = [pickler.dumps(s) for s in schemas]
      self.pickled_annotations = [pickler.dumps(a) for a in annotations]

    def setup(self):
      main_session = importlib.import_module('__main__')
      for pickled_schema, pickled_annotation in zip(
          self.pickled_schemas, self.pickled_annotations):
        schema = pickler.loads(pickled_schema)
        schema.__annotations__ = pickler.loads(pickled_annotation)
        if not hasattr(main_session, schema.__name__) or not hasattr(
            getattr(main_session, schema.__name__), '__annotations__'):
          # Restore the schema in the main session on the [remote] worker.
          setattr(main_session, schema.__name__, schema)
        register_coder_for_schema(schema)

    def process(self, e):
      yield e

  def expand(self, source):
    """Applies the SQL transform. If a PCollection uses a schema defined in
    the main session, use the additional DoFn to restore it on the worker."""
    if isinstance(source, dict):
      schema_loaded = {
          tag: pcoll | 'load_schemas_{}_tag_{}_{}'.format(
              self.output_name, tag, self.execution_count) >> beam.ParDo(
                  self._SqlTransformDoFn(self.schemas, self.schema_annotations))
          if pcoll.element_type in self.schemas else pcoll
          for tag, pcoll in source.items()
      }
    elif isinstance(source, beam.pvalue.PCollection):
      schema_loaded = source | 'load_schemas_{}_{}'.format(
          self.output_name, self.execution_count) >> beam.ParDo(
              self._SqlTransformDoFn(self.schemas, self.schema_annotations)
          ) if source.element_type in self.schemas else source
    else:
      raise ValueError(
          '{} should be either a single PCollection or a dict of named '
          'PCollections.'.format(source))
    return schema_loaded | 'beam_sql_{}_{}'.format(
        self.output_name, self.execution_count) >> SqlTransform(self.query)


@dataclass
class SqlChain:
  """A chain of SqlNodes.

  Attributes:
    nodes: all nodes by their output_names.
    root: the first SqlNode applied chronologically.
    current: the last node applied.
    user_pipeline: the user defined pipeline this chain originates from. If
      None, the whole chain just computes from raw values in queries.
      Otherwise, at least some of the nodes in chain has queried against
      PCollections.
  """
  nodes: Dict[str, SqlNode] = None
  root: Optional[SqlNode] = None
  current: Optional[SqlNode] = None
  user_pipeline: Optional[beam.Pipeline] = None

  def __post_init__(self):
    if not self.nodes:
      self.nodes = {}

  @progress_indicated
  def to_pipeline(self) -> beam.Pipeline:
    """Converts the chain into a beam pipeline."""
    pipeline_to_execute = self.root.to_pipeline(self.user_pipeline)
    # The pipeline definitely contains external transform: SqlTransform.
    pipeline_to_execute.contains_external_transforms = True
    return pipeline_to_execute

  def append(self, node: SqlNode) -> 'SqlChain':
    """Appends a node to the chain."""
    if self.current:
      self.current.next = node
    else:
      self.root = node
    self.current = node
    self.nodes[node.output_name] = node
    return self

  def get(self, output_name: str) -> Optional[SqlNode]:
    """Gets a node from the chain based on the given output_name."""
    return self.nodes.get(output_name, None)
