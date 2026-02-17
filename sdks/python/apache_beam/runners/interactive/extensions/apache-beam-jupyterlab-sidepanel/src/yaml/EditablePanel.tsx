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
import { Node } from '@xyflow/react';
import '../../style/yaml/YamlEditor.css';
import { transformEmojiMap } from './EmojiMap';

type EditableKeyValuePanelProps = {
  node: Node;
  onChange: (newData: Record<string, any>) => void;
  depth?: number;
};

type EditableKeyValuePanelState = {
  localData: Record<string, any>;
  collapsedKeys: Set<string>;
};

/**
 * An editable key-value panel component for displaying
 * and modifying node properties.
 *
 * Features:
 * - Nested object support with collapsible sections
 * - Real-time key-value editing with validation
 * - Dynamic field addition and deletion
 * - Support for multi-line text values
 * - Object conversion for nested structures
 * - Reference documentation integration
 * - Visual hierarchy with depth-based indentation
 * - Interactive UI with hover effects and transitions
 *
 * State Management:
 * - localData: Local copy of the node data being edited
 * - collapsedKeys: Set of keys that are currently collapsed
 *
 * Props:
 * @param {Node} node - The node is data to be edited
 * @param {(data: Record<string, any>) => void} onChange -
 *  Callback for data changes
 * @param {number} [depth=0] - Current nesting depth for recursive rendering
 *
 * Methods:
 * - toggleCollapse: Toggles collapse state of nested objects
 * - handleKeyChange: Updates keys with validation
 * - handleValueChange: Updates values in the local data
 * - handleDelete: Removes key-value pairs
 * - handleAddPair: Adds new key-value pairs
 * - convertToObject: Converts primitive values to objects
 * - renderValueEditor: Renders appropriate input based on value type
 *
 * UI Features:
 * - Collapsible nested object sections
 * - Multi-line text support for complex values
 * - Add/Delete buttons for field management
 * - Reference documentation links
 * - Visual feedback for user interactions
 * - Responsive design with proper spacing
 */
export class EditableKeyValuePanel extends React.Component<
  EditableKeyValuePanelProps,
  EditableKeyValuePanelState
> {
  static defaultProps = {
    depth: 0
  };

  constructor(props: EditableKeyValuePanelProps) {
    super(props);
    this.state = {
      localData: { ...(props.node ? props.node.data : {}) },
      collapsedKeys: new Set()
    };
  }

  componentDidUpdate(prevProps: EditableKeyValuePanelProps) {
    if (prevProps.node !== this.props.node && this.props.node) {
      this.setState({ localData: { ...(this.props.node.data ?? {}) } });
    }
  }

  toggleCollapse = (key: string) => {
    this.setState(({ collapsedKeys }) => {
      const newSet = new Set(collapsedKeys);
      newSet.has(key) ? newSet.delete(key) : newSet.add(key);
      return { collapsedKeys: newSet };
    });
  };

  handleKeyChange = (oldKey: string, newKey: string) => {
    newKey = newKey.trim();
    if (newKey === oldKey || newKey === '') {
      return alert('Invalid Key!');
    }
    if (newKey in this.state.localData) {
      return alert('Duplicated Key!');
    }

    const newData: Record<string, any> = {};
    for (const [k, v] of Object.entries(this.state.localData)) {
      newData[k === oldKey ? newKey : k] = v;
    }

    this.setState({ localData: newData }, () => this.props.onChange(newData));
  };

  handleValueChange = (key: string, newValue: any) => {
    const newData = { ...this.state.localData, [key]: newValue };
    this.setState({ localData: newData }, () => this.props.onChange(newData));
  };

  handleDelete = (key: string) => {
    const { [key]: _, ...rest } = this.state.localData;
    void _;
    this.setState({ localData: rest }, () => this.props.onChange(rest));
  };

  handleAddPair = () => {
    let i = 1;
    const baseKey = 'newKey';
    while (`${baseKey}${i}` in this.state.localData) {
      i++;
    }
    const newKey = `${baseKey}${i}`;
    const newData = { ...this.state.localData, [newKey]: '' };
    this.setState({ localData: newData }, () => this.props.onChange(newData));
  };

  convertToObject = (key: string) => {
    if (
      typeof this.state.localData[key] === 'object' &&
      this.state.localData[key] !== null
    ) {
      return;
    }
    const newData = { ...this.state.localData, [key]: {} };
    this.setState({ localData: newData }, () => this.props.onChange(newData));
    this.setState(({ collapsedKeys }) => {
      const newSet = new Set(collapsedKeys);
      newSet.delete(key);
      return { collapsedKeys: newSet };
    });
  };

  renderValueEditor = (key: string, value: any) => {
    const isMultiline =
      key === 'callable' || (typeof value === 'string' && value.includes('\n'));

    return isMultiline ? (
      <textarea
        value={value}
        onChange={e => this.handleValueChange(key, e.target.value)}
        className="editor-input"
        style={{ minHeight: 100 }}
      />
    ) : (
      <input
        type="text"
        value={value}
        onChange={e => this.handleValueChange(key, e.target.value)}
        className="editor-input"
      />
    );
  };

  render() {
    const { localData, collapsedKeys } = this.state;
    const depth = this.props.depth ?? 0;

    return (
      <div style={{ fontFamily: 'monospace', fontSize: 14 }}>
        {Object.entries(localData).map(([key, value]) => {
          const isObject =
            typeof value === 'object' &&
            value !== null &&
            !Array.isArray(value);
          const isCollapsed = collapsedKeys.has(key);

          return (
            <div key={key} style={{ marginBottom: 4 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                {/* Toggle Button or Spacer */}
                {isObject ? (
                  <button
                    onClick={() => this.toggleCollapse(key)}
                    style={{
                      width: 24,
                      height: 32,
                      cursor: 'pointer',
                      border: 'none',
                      background: 'none',
                      fontWeight: 'bold',
                      userSelect: 'none',
                      flexShrink: 0
                    }}
                  >
                    {isCollapsed ? '▶' : '▼'}
                  </button>
                ) : (
                  <div style={{ width: 24, height: 32 }} />
                )}

                {/* Key input */}
                <input
                  type="text"
                  value={key}
                  onChange={e => this.handleKeyChange(key, e.target.value)}
                  className="editor-input"
                  style={{
                    width: 120,
                    height: 32,
                    boxSizing: 'border-box',
                    padding: '4px 6px',
                    borderRadius: 4,
                    border: '1px solid #ccc',
                    fontFamily: 'inherit',
                    fontSize: 'inherit'
                  }}
                />

                {/* Value input or collapsed preview */}
                <div style={{ flexGrow: 1 }}>
                  {isObject ? (
                    isCollapsed ? (
                      <span style={{ color: '#888' }}>{'{...}'}</span>
                    ) : (
                      <span style={{ color: '#444', fontStyle: 'italic' }}>
                        {'{...}'}
                      </span>
                    )
                  ) : (
                    this.renderValueEditor(key, value)
                  )}
                </div>

                {/* Action buttons */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  {!isObject && (
                    <button
                      onClick={() => this.convertToObject(key)}
                      style={{
                        width: 70,
                        height: 32,
                        padding: '4px 8px',
                        borderRadius: 4,
                        border: '1px solid #4caf50',
                        backgroundColor: '#e8f5e9',
                        color: '#2e7d32',
                        cursor: 'pointer'
                      }}
                    >
                      + Sub
                    </button>
                  )}
                  <button
                    onClick={() => this.handleDelete(key)}
                    style={{
                      height: 32,
                      padding: '4px 8px',
                      borderRadius: 4,
                      border: '1px solid #f44336',
                      backgroundColor: '#ffebee',
                      color: '#b71c1c',
                      cursor: 'pointer'
                    }}
                  >
                    ×
                  </button>
                </div>
              </div>

              {isObject && !isCollapsed && (
                <div
                  style={{
                    marginLeft: 20,
                    marginTop: 4,
                    borderLeft: '1px solid #ccc',
                    paddingLeft: 8
                  }}
                >
                  <EditableKeyValuePanel
                    node={{ id: key, data: value } as Node}
                    onChange={newVal => this.handleValueChange(key, newVal)}
                    depth={depth + 1}
                  />
                </div>
              )}
            </div>
          );
        })}

        <button
          onClick={this.handleAddPair}
          style={{
            marginTop: 8,
            padding: '6px 12px',
            borderRadius: 4,
            border: '1px solid #2196f3',
            backgroundColor: '#e3f2fd',
            color: '#0d47a1',
            cursor: 'pointer'
          }}
        >
          + Add {depth > 0 ? 'Nested ' : ''}Field
        </button>

        {/* Reference Doc */}
        {depth === 0 && (
          <div
            style={{
              marginTop: 14,
              padding: '14px 20px',
              borderRadius: 12,
              background: 'linear-gradient(135deg, #f7f9fc, #e3f0ff)',
              border: '1px solid #4facfe',
              boxShadow: '0 4px 12px rgba(0, 0, 0, 0.08)',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              fontFamily: 'sans-serif',
              fontSize: 14,
              color: '#0d47a1',
              transition: 'transform 0.15s ease, box-shadow 0.15s ease'
            }}
            onMouseEnter={e => {
              e.currentTarget.style.transform = 'translateY(-2px)';
              e.currentTarget.style.boxShadow =
                '0 6px 16px rgba(0, 0, 0, 0.12)';
            }}
            onMouseLeave={e => {
              e.currentTarget.style.transform = 'translateY(0)';
              e.currentTarget.style.boxShadow =
                '0 4px 12px rgba(0, 0, 0, 0.08)';
            }}
          >
            {/* Emoji + label */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <div
                style={{
                  fontSize: 22,
                  width: 28,
                  height: 28,
                  display: 'flex',
                  justifyContent: 'center',
                  alignItems: 'center'
                }}
              >
                {transformEmojiMap[localData.label || ''] || '📄'}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <span
                  style={{ fontWeight: 600, fontSize: 14, color: '#0d47a1' }}
                >
                  {localData.label}
                </span>
                <span style={{ fontSize: 12, color: '#555' }}>
                  Reference for Beam YAML transform
                </span>
              </div>
            </div>

            {/* Button */}
            <a
              href={`https://beam.apache.org/releases/yamldoc/current/#${encodeURIComponent(
                localData.label?.toLowerCase() || ''
              )}`}
              target="_blank"
              rel="noopener noreferrer"
              style={{
                padding: '6px 14px',
                borderRadius: 6,
                backgroundColor: '#2196f3',
                color: 'white',
                fontWeight: 500,
                fontSize: 13,
                textDecoration: 'none',
                boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
                transition: 'all 0.2s ease'
              }}
              onMouseEnter={e => {
                e.currentTarget.style.backgroundColor = '#1976d2';
                e.currentTarget.style.transform = 'translateY(-1px)';
                e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,0,0,0.15)';
              }}
              onMouseLeave={e => {
                e.currentTarget.style.backgroundColor = '#2196f3';
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = '0 2px 4px rgba(0,0,0,0.1)';
              }}
            >
              Open Doc
            </a>
          </div>
        )}
      </div>
    );
  }
}
