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

import React from 'react';
import { ISessionContext } from '@jupyterlab/apputils';
import { Card } from '@rmwc/card';
import '@rmwc/textfield/styles';
import '@rmwc/grid/styles';
import '@rmwc/card/styles';
import Split from 'react-split';
import { debounce } from 'lodash';

import { Node, Edge } from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import '../../style/yaml/Yaml.css';


import { YamlEditor } from './YamlEditor';
import { EditableKeyValuePanel } from './EditablePanel';
import { FlowEditor } from './YamlFlow';
import { ApiResponse } from './DataType';
import { nodeWidth, nodeHeight } from './DataType';

interface YamlProps {
    sessionContext: ISessionContext;
}

interface YamlState {
    yamlContent: string;
    elements: any;
    selectedNode: Node | null;
    errors: string[],
    isDryRunMode: boolean;
    Nodes: Node[];
    Edges: Edge[];
}

const initialNodes: Node[] = [
    { id: '0', width: nodeWidth, position: { x: 0, y: 0 }, type: 'input', data: { label: 'Input' } },
    { id: '1', width: nodeWidth, position: { x: 0, y: 100 }, type: 'default', data: { label: '1' } },
    { id: '2', width: nodeWidth, position: { x: 0, y: 200 }, type: 'default', data: { label: '2' } },
    { id: '3', width: nodeWidth, position: { x: 0, y: 300 }, type: 'output', data: { label: 'Output' } },
];

const initialEdges: Edge[] = [{ id: 'e0-1', source: '0', target: '1' },
{ id: 'e1-2', source: '1', target: '2' }, { id: 'e2-3', source: '2', target: '3' }
];

/**
 * A YAML pipeline editor component with integrated flow visualization.
 * 
 * Features:
 * - Three-panel layout with YAML editor, flow diagram, and node properties
 * - Real-time YAML validation and error display
 * - Automatic flow diagram generation from YAML content
 * - Interactive node selection and editing
 * - Dry run mode support for pipeline testing
 * - Kernel-based YAML parsing using Apache Beam utilities
 * - Debounced updates to optimize performance
 * - Split-pane resizable interface
 * 
 * State Management:
 * - yamlContent: Current YAML text content
 * - elements: Combined nodes and edges for the flow diagram
 * - selectedNode: Currently selected node in the flow diagram
 * - errors: Array of validation errors
 * - Nodes: Array of flow nodes
 * - Edges: Array of flow edges
 * - isDryRunMode: Flag for dry run mode state
 * 
 * Props:
 * @param {YamlProps} props - Component props including sessionContext for kernel communication
 * 
 * Methods:
 * - handleNodeClick: Handles node selection in the flow diagram
 * - handleYamlChange: Debounced handler for YAML content changes
 * - validateAndRenderYaml: Validates YAML and updates the flow diagram
 */
export class Yaml extends React.Component<YamlProps, YamlState> {
    constructor(props: YamlProps) {
        super(props);
        this.state = { yamlContent: '', elements: [], selectedNode: null, errors: [], Nodes: initialNodes, Edges: initialEdges, isDryRunMode: false };
    }

    componentDidMount(): void {
        this.props.sessionContext.ready.then(() => {
            const kernel = this.props.sessionContext.session?.kernel;
            if (!kernel) {
                console.error('Kernel is not available even after ready');
                return;
            }

            console.log('Kernel is ready:', kernel.name);
        });
    }

    componentDidUpdate(prevProps: YamlProps, prevState: YamlState) {
        if (prevState.selectedNode !== this.state.selectedNode) {
            console.log('selectedNodeData changed:', this.state.selectedNode);
        }
    }

    handleNodeClick = (node: Node) => {
        this.setState({
            selectedNode: node
        });
    };

    //debounce methods to prevent excessive rendering
    private handleYamlChange = debounce((value?: string) => {
        const yamlText = value || '';
        this.setState({ yamlContent: yamlText });
        this.validateAndRenderYaml(yamlText);
    }, 2000);

    validateAndRenderYaml(yamlText: string) {
        const escapedYaml = yamlText.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        const code = `
from apache_beam_jupyterlab_sidepanel.yaml_parse_utils import parse_beam_yaml
print(parse_beam_yaml("""${escapedYaml}""", isDryRunMode=${this.state.isDryRunMode ? "True" : "False"}))
`.trim();
        const session = this.props.sessionContext.session;
        if (!session?.kernel) {
            console.error('No kernel available');
            return;
        }

        // Clear previous state immediately
        this.setState({
            Nodes: [],
            Edges: [],
            selectedNode: null,
            errors: []
        });

        const future = session.kernel.requestExecute({ code });

        // Handle kernel execution results
        future.onIOPub = (msg) => {
            if (msg.header.msg_type === 'stream') {
                const content = msg.content as { name: string; text: string };
                const output = content.text.trim();

                console.log('Received output:', output);

                try {
                    const result: ApiResponse = JSON.parse(output);
                    console.log('Received result:', result);

                    if (result.error) {
                        this.setState({
                            elements: [],
                            selectedNode: null,
                            errors: [result.error]
                        });
                    } else {

                        const flowNodes: Node[] = result.data.nodes.map(node => ({
                            id: node.id,
                            type: node.type,
                            width: nodeWidth,
                            height: nodeHeight,
                            position: { x: 0, y: 0 }, // Will be auto-layouted
                            data: {
                                label: node.label,
                                ...node // include all original properties
                            }
                        }));

                        // Transform edges for React Flow
                        const flowEdges: Edge[] = result.data.edges.map(edge => ({
                            id: `${edge.source}-${edge.target}`,
                            source: edge.source,
                            target: edge.target,
                            animated: edge.label === 'pipeline_entry',
                            label: edge.label,
                            type: 'default' // or your custom edge type
                        }));

                        this.setState({
                            Nodes: flowNodes,
                            Edges: flowEdges,
                            errors: []
                        });
                    }
                } catch (err) {
                    this.setState({
                        elements: [],
                        selectedNode: null,
                        errors: [output]
                    });
                }
            }
        };
    }

    render(): React.ReactNode {
        return (
            <div style={{
                height: '100vh',
                width: '100vw',
            }}>
                <Split
                    direction="horizontal"
                    sizes={[25, 35, 20]} // L/C/R
                    minSize={100}
                    gutterSize={6}
                    className='split-pane'
                >
                    {/* Left */}
                    <div style={{
                        height: '100%',
                        overflow: 'auto',
                        minWidth: '100px'
                    }}>
                        <YamlEditor
                            value={this.state.yamlContent}
                            onChange={(value) => {
                                // Clear old errors & Handle new errors
                                this.setState({ errors: [] });
                                this.handleYamlChange(value || '');
                            }}
                            errors={this.state.errors}
                            showConsole={true}
                            onDryRunModeChange={(newValue) => this.setState({ isDryRunMode: newValue })}
                        />
                    </div>

                    <div style={{
                        padding: '1rem',
                        height: '100%',
                    }}>
                        <Card style={{
                            height: '100%',
                            display: 'flex',
                            flexDirection: 'column'
                        }}>
                            <div className="w-full h-full bg-gray-50 dark:bg-zinc-900 text-sm font-sans" style={{ flex: 1, minHeight: 0 }}>
                                <FlowEditor
                                    Nodes={this.state.Nodes}
                                    Edges={this.state.Edges}
                                    onNodesUpdate={(nodes: Node[]) => this.setState({ Nodes: nodes })}
                                    onEdgesUpdate={(edges: Edge[]) => this.setState({ Edges: edges })}
                                    onNodeClick={this.handleNodeClick}
                                />
                            </div>
                        </Card>
                    </div>

                    {/* Right */}
                    <div style={{
                        height: '100%',
                        overflow: 'auto',
                        padding: '12px',
                        boxSizing: 'border-box'
                    }}>
                        <EditableKeyValuePanel
                            node={this.state.selectedNode}
                            // TODO: implement onChange to update node data from Panel
                            onChange={() => { }}
                        />
                    </div>
                </Split>
            </div>
        );

    }


}
