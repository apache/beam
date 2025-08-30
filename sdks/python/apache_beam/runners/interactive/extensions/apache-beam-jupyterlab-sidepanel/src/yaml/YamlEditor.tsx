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

import React, { useState, useRef } from 'react';
import Editor from '@monaco-editor/react';
interface YamlEditorProps {
    value: string;
    onChange: (value: string | undefined) => void;
    onDryRunModeChange: (value: boolean) => void;
    errors?: string[];
    showConsole?: boolean;
    consoleHeight?: number;
}

/**
 * A YAML editor component built with React and Monaco Editor.
 * 
 * Features:
 * - Full YAML syntax highlighting and validation
 * - Adjustable font size (10px-24px) with zoom controls
 * - Word wrap toggle
 * - File download and upload capabilities
 * - Dry run mode toggle for testing
 * - Error console with detailed validation messages
 * 
 * Props:
 * @param {string} value - Current YAML content
 * @param {(isDryRun: boolean) => void} onDryRunModeChange - Callback when dry run mode changes
 * @param {(value: string) => void} onChange - Callback when content changes
 * @param {string[]} [errors=[]] - Array of error messages to display
 * @param {boolean} [showConsole=true] - Whether to show the error console
 * @param {number} [consoleHeight=200] - Height of the error console in pixels
 */
export const YamlEditor: React.FC<YamlEditorProps> = ({
    value,
    onDryRunModeChange,
    onChange,
    errors = [],
    showConsole = true,
    consoleHeight = 200
}) => {
    const [fontSize, setFontSize] = useState(14);
    const [wordWrap, setWordWrap] = useState(true);
    const [isDryRunMode, setDryRunMode] = useState(false);
    const [isFontMenuOpen, setFontMenuOpen] = useState<boolean>(false);
    const fileInputRef = useRef<HTMLInputElement>(null);

    const zoomIn = () => setFontSize(prev => Math.min(prev + 1, 24));
    const zoomOut = () => setFontSize(prev => Math.max(prev - 1, 10));
    const handleFontSizeChange = (e: React.ChangeEvent<HTMLInputElement>) => setFontSize(Number(e.target.value));

    // Download YAML File
    const handleDownload = () => {
        const blob = new Blob([value], { type: 'text/yaml;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = 'pipeline.yaml';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    };

    // Open YAML File
    const handleOpenFile = (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (ev) => {
            const content = ev.target?.result;
            if (typeof content === 'string') onChange(content);
        };
        reader.readAsText(file);
        e.target.value = '';
    };

    return (
        <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: '#1e1e1e' }}>
            {/* ================ Toolbar ================ */}
            <div style={{
                padding: '8px',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                borderBottom: '1px solid #333',
                gap: 8
            }}>
                {/* Fonts */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <button
                        onClick={() => setFontMenuOpen(!isFontMenuOpen)}
                        style={{
                            background: 'transparent',
                            border: '1px solid #555',
                            color: '#ddd',
                            padding: '4px 8px',
                            borderRadius: '4px',
                            cursor: 'pointer'
                        }}
                    >
                        Font {fontSize}px
                    </button>
                    {isFontMenuOpen && (
                        <div style={{
                            position: 'absolute',
                            top: '40px',
                            left: '8px',
                            background: '#252526',
                            padding: '8px',
                            borderRadius: '4px',
                            boxShadow: '0 2px 8px rgba(0,0,0,0.3)',
                            zIndex: 100
                        }}>
                            <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                                <button onClick={zoomOut} style={{ background: '#333', border: 'none', color: '#fff', width: 24, height: 24, borderRadius: 4, cursor: 'pointer' }}>-</button>
                                <input type="range" min="10" max="24" value={fontSize} onChange={handleFontSizeChange} style={{ margin: '0 8px', width: 100 }} />
                                <button onClick={zoomIn} style={{ background: '#333', border: 'none', color: '#fff', width: 24, height: 24, borderRadius: 4, cursor: 'pointer' }}>+</button>
                            </div>
                            <div style={{ color: '#999', fontSize: 12 }}>Current: {fontSize}px</div>
                        </div>
                    )}
                </div>

                {/* Autowrap & DryRun */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                    <label style={{ display: 'flex', alignItems: 'center', color: '#ddd', cursor: 'pointer' }}>
                        <input type="checkbox" checked={wordWrap} onChange={() => setWordWrap(!wordWrap)} style={{ marginRight: 6 }} /> Autowrap
                    </label>
                    <label style={{ display: 'flex', alignItems: 'center', color: '#ddd', cursor: 'pointer' }}>
                        <input type="checkbox" checked={isDryRunMode} onChange={() => { onDryRunModeChange(!isDryRunMode); setDryRunMode(!isDryRunMode); }} style={{ marginRight: 6 }} /> Dry Run Mode
                    </label>
                </div>

                {/* Download / Open */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <button
                        onClick={handleDownload}
                        style={{ padding: '4px 10px', borderRadius: 4, border: '1px solid #2196f3', background: '#2196f3', color: '#fff', cursor: 'pointer' }}
                    >
                        Download
                    </button>
                    <button
                        onClick={() => fileInputRef.current?.click()}
                        style={{ padding: '4px 10px', borderRadius: 4, border: '1px solid #4caf50', background: '#4caf50', color: '#fff', cursor: 'pointer' }}
                    >
                        Open
                    </button>
                    <input
                        ref={fileInputRef}
                        type="file"
                        accept=".yaml,.yml"
                        style={{ display: 'none' }}
                        onChange={handleOpenFile}
                    />
                </div>
            </div>
            {/* ================ ~Toolbar ================ */}

            {/* ================ Code Editor ================ */}
            <div style={{ flex: 1, minHeight: 0 }}>
                <Editor
                    height='98%'
                    language="yaml"
                    value={value}
                    theme="vs-dark"
                    onChange={onChange}
                    options={{
                        fontSize,
                        fontFamily: "monospace",
                        wordWrap: wordWrap ? 'on' : 'off',
                        minimap: { enabled: false },
                        scrollBeyondLastLine: false,
                        automaticLayout: true,
                        lineNumbers: 'on',
                        tabSize: 2
                    }}
                />
            </div>
            {/* ================ ~Code Editor ================ */}

            {/* ================ Console ================ */}
            {showConsole && (
                <div style={{
                    height: consoleHeight,
                    background: '#1e1e1e',
                    borderTop: '1px solid #ff6b6b',
                    overflow: 'auto',
                    padding: 12,
                    fontFamily: 'monospace',
                    fontSize: 13,
                    whiteSpace: 'pre'
                }}>
                    <div style={{ color: errors.length ? '#f48771' : '#888', marginBottom: errors.length ? 12 : 0, fontWeight: 500 }}>
                        {errors.length ? `❌` : '✅ YAML Format Correct'}
                    </div>
                    {errors.map((error, i) => (
                        <pre key={i} style={{
                            color: '#f48771',
                            margin: '8px 0',
                            padding: 0,
                            backgroundColor: 'transparent',
                            border: 'none',
                            overflow: 'visible',
                            whiteSpace: 'pre-wrap',
                            wordBreak: 'break-all',
                            fontFamily: 'inherit'
                        }}>{error}</pre>
                    ))}
                </div>
            )}
            {/* ================ ~Console ================ */}
        </div>
    );
};