#  Licensed under the Apache License, Version 2.0 (the 'License'); you may not
#  use this file except in compliance with the License. You may obtain a copy of
#  the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
#  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#  License for the specific language governing permissions and limitations under
#  the License.

import dataclasses
import json
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import TypedDict

import yaml

import apache_beam as beam
from apache_beam.yaml.main import build_pipeline_components_from_yaml

# ======================== Type Definitions ========================


@dataclass
class NodeData:
  id: str
  label: str
  type: str = ""

  def __post_init__(self):
    # Ensure ID is not empty
    if not self.id:
      raise ValueError("Node ID cannot be empty")


@dataclass
class EdgeData:
  source: str
  target: str
  label: str = ""

  def __post_init__(self):
    if not self.source or not self.target:
      raise ValueError("Edge source and target cannot be empty")


class FlowGraph(TypedDict):
  nodes: List[Dict[str, Any]]
  edges: List[Dict[str, Any]]


# ======================== Main Function ========================


def parse_beam_yaml(yaml_str: str, isDryRunMode: bool = False) -> str:
  """
    Parse Beam YAML and convert to flow graph data structure
    
    Args:
        yaml_str: Input YAML string
        
    Returns:
        Standardized response format:
        - Success: {'status': 'success', 'data': {...}, 'error': None}
        - Failure: {'status': 'error', 'data': None, 'error': 'message'}
    """
  # Phase 1: YAML Parsing
  try:
    parsed_yaml = yaml.safe_load(yaml_str)
    if not parsed_yaml or 'pipeline' not in parsed_yaml:
      return build_error_response(
          "Invalid YAML structure: missing 'pipeline' section")
  except yaml.YAMLError as e:
    return build_error_response(f"YAML parsing error: {str(e)}")

  # Phase 2: Pipeline Validation
  try:
    options, constructor = build_pipeline_components_from_yaml(
        yaml_str,
        [],
        validate_schema='per_transform'
    )
    if isDryRunMode:
      with beam.Pipeline(options=options) as p:
        constructor(p)
  except Exception as e:
    return build_error_response(f"Pipeline validation failed: {str(e)}")

  # Phase 3: Graph Construction
  try:
    pipeline = parsed_yaml['pipeline']
    transforms = pipeline.get('transforms', [])

    nodes: List[NodeData] = []
    edges: List[EdgeData] = []

    nodes.append(NodeData(id='0', label='Input', type='input'))
    nodes.append(NodeData(id='1', label='Output', type='output'))

    # Process transform nodes
    for idx, transform in enumerate(transforms):
      if not isinstance(transform, dict):
        continue

      payload = {k: v for k, v in transform.items() if k not in {"type"}}

      node_id = f"t{idx}"
      node_data = NodeData(
          id=node_id,
          label=transform.get('type', 'unnamed'),
          type='default',
          **payload)
      nodes.append(node_data)

      # Create connections between nodes
      if idx > 0:
        edges.append(
            EdgeData(source=f"t{idx-1}", target=node_id, label='chain'))

    if transforms:
      edges.append(EdgeData(source='0', target='t0', label='start'))
      edges.append(EdgeData(source=node_id, target='1', label='stop'))

    def to_dict(node):
      if hasattr(node, '__dataclass_fields__'):
        return dataclasses.asdict(node)
      return node

    nodes_serializable = [to_dict(n) for n in nodes]

    return build_success_response(
        nodes=nodes_serializable, edges=[dataclasses.asdict(e) for e in edges])

  except Exception as e:
    return build_error_response(f"Graph construction failed: {str(e)}")


# ======================== Utility Functions ========================


def build_success_response(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> str:
  """Build success response"""
  return json.dumps({'data': {'nodes': nodes, 'edges': edges}, 'error': None})


def build_error_response(error_msg: str) -> str:
  """Build error response"""
  return json.dumps({'data': None, 'error': error_msg})


if __name__ == "__main__":
  # Example usage
  example_yaml = """
pipeline:
  transforms:
    - type: ReadFromCsv
      name: A
      config:
        path: /path/to/input*.csv
    - type: WriteToJson
      name: B
      config:
        path: /path/to/output.json
      input: ReadFromCsv
    - type: Join
      input: [A, B]
    """

  response = parse_beam_yaml(example_yaml, isDryRunMode=False)
  print(response)
