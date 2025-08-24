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

export const nodeWidth = 320;
export const nodeHeight = 100;

export interface NodeData {
    id: string;
    label: string;
    type?: string;
    [key: string]: any;
}

export interface EdgeData {
    source: string;
    target: string;
    label?: string;
}

export interface FlowGraph {
    nodes: NodeData[];
    edges: EdgeData[];
}

export interface ApiResponse {
    data: FlowGraph | null;
    error: string | null;
}