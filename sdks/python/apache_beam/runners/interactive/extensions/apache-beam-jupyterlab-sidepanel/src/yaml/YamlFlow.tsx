// Licensed under the Apache License, Version 2.0 (the 'License'); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

import React, { useEffect, useCallback, useMemo } from 'react';
import {
  ReactFlow,
  useNodesState,
  useEdgesState,
  addEdge,
  MiniMap,
  Controls,
  Background,
  Panel,
  Node,
  Edge,
  applyNodeChanges,
  NodeChange,
  Connection
} from '@xyflow/react';
import { debounce } from 'lodash';
import dagre from 'dagre';
import {
  DefaultNode,
  InputNode,
  OutputNode,
  AnimatedSVGEdge
} from './CustomStyle';
import { nodeWidth, nodeHeight } from './DataType';

import '@xyflow/react/dist/style.css';

interface IFlowEditorProps {
  Nodes: Node[];
  Edges: Edge[];
  onNodesUpdate?: (nodes: Node[]) => void;
  onEdgesUpdate?: (edges: Edge[]) => void;
  onNodeClick?: (node: Node) => void;
  debounceWait?: number;
}

/**
 * A flow diagram editor component built with React Flow.
 *
 * Features:
 * - Interactive node-based flow diagram editor
 * - Auto-layout functionality using Dagre graph layout
 * - Support for different node types (default, input, output)
 * - Animated edge connections
 * - Mini-map and controls for navigation
 * - Debounced updates to optimize performance
 * - Real-time node and edge manipulation
 *
 * Props:
 * @param {Node[]} Nodes - Initial array of nodes
 * @param {Edge[]} Edges - Initial array of edges
 * @param {(nodes: Node[]) => void} onNodesUpdate -
 * Callback when nodes are updated
 * @param {(edges: Edge[]) => void} onEdgesUpdate -
 * Callback when edges are updated
 * @param {(event: React.MouseEvent, node: Node) => void} onNodeClick -
 * Callback when a node is clicked
 * @param {number} [debounceWait=500] -
 * Debounce wait time in milliseconds for updates
 */
export const FlowEditor: React.FC<IFlowEditorProps> = ({
  Nodes,
  Edges,
  onNodesUpdate,
  onEdgesUpdate,
  onNodeClick,
  debounceWait = 500 //Default debounce wait time
}: IFlowEditorProps) => {
  const [nodes, setNodes, _] = useNodesState(Nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(Edges);

  void _;
  // Debounce callback
  const debouncedNodesUpdate = useMemo(
    () =>
      debounce((nodes: Node[]) => {
        onNodesUpdate?.(nodes);
      }, debounceWait),
    [onNodesUpdate, debounceWait]
  );

  const debouncedEdgesUpdate = useMemo(
    () =>
      debounce((edges: Edge[]) => {
        onEdgesUpdate?.(edges);
      }, debounceWait),
    [onEdgesUpdate, debounceWait]
  );

  const onConnect = useCallback(
    (params: Connection) => setEdges(eds => addEdge(params, eds)),
    [setEdges]
  );

  // Listen initialNodes/initialEdges changes and update state accordingly
  useEffect(() => {
    setNodes(Nodes);
  }, [Nodes]);

  useEffect(() => {
    setEdges(Edges);
  }, [Edges]);

  const handleNodeClick = useCallback(
    (event: React.MouseEvent, node: Node) => {
      onNodeClick?.(node);
    },
    [onNodeClick]
  );

  const applyAutoLayout = useCallback(
    (nodes: Node[], edges: Edge[]): Node[] => {
      const g = new dagre.graphlib.Graph();
      g.setDefaultEdgeLabel(() => ({}));
      g.setGraph({
        rankdir: 'TB',
        ranksep: 100
      }); // TB = Top to Bottom. Use LR for Left to Right

      nodes.forEach(node => {
        g.setNode(node.id, { width: nodeWidth, height: nodeHeight });
      });

      edges.forEach(edge => {
        g.setEdge(edge.source, edge.target);
      });

      dagre.layout(g);

      return nodes.map(node => {
        const pos = g.node(node.id);
        return {
          ...node,
          position: {
            x: pos.x - nodeWidth / 2,
            y: pos.y - nodeHeight / 2
          }
        };
      });
    },
    []
  );

  const handleNodesChange = useCallback(
    (changes: NodeChange[]) => {
      const updatedNodes = applyNodeChanges(changes, nodes);

      // Judge whether the node data is changed or not
      // except position / dragging / selected）
      const structuralChanges = changes.filter(
        c => !['position', 'dragging', 'selected', 'select'].includes(c.type)
      );

      if (structuralChanges.length > 0) {
        // Only when there are structural changes, we apply auto-layout
        const autoLayouted = applyAutoLayout(updatedNodes, edges);
        setNodes(autoLayouted);
        onNodesUpdate?.(autoLayouted);
      } else {
        // Dragging or selection changes, just update normally
        setNodes(updatedNodes);
      }
    },
    [nodes, edges, onNodesUpdate, applyAutoLayout]
  );

  const NodeTypes = {
    default: DefaultNode,
    input: InputNode,
    output: OutputNode
  };

  // notify parent whenever nodes/edges change
  useEffect(() => {
    debouncedNodesUpdate(nodes);
    return () => debouncedNodesUpdate.cancel();
  }, [nodes, debouncedNodesUpdate]);

  useEffect(() => {
    debouncedEdgesUpdate(edges);
    return () => debouncedEdgesUpdate.cancel();
  }, [edges, debouncedEdgesUpdate]);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={NodeTypes}
      edgeTypes={{ default: AnimatedSVGEdge }}
      onNodesChange={handleNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      onNodeClick={handleNodeClick}
      fitView
    >
      <Panel position="top-right">
        <button
          onClick={() => {
            const newNodes = applyAutoLayout(nodes, edges);
            setNodes(newNodes);
          }}
          className="px-3 py-1 
          bg-blue-600 text-white rounded-md shadow 
          hover:bg-blue-700 transition-all duration-200"
        >
          Auto Layout
        </button>
      </Panel>
      <MiniMap />
      <Controls />
      <Background />
    </ReactFlow>
  );
};
