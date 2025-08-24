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

import React, { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { EdgeProps, BaseEdge, getSmoothStepPath } from '@xyflow/react';
import { NodeData } from './DataType';
import { transformEmojiMap } from "./EmojiMap";

export function DefaultNode({ data }: { data: NodeData }) {
    const emoji = data.label ? transformEmojiMap[data.label] || "📦" : data.emoji || "📦";
    const typeClass = data.type ? `custom-node-${data.type}` : "";

    return (
        <div className={`custom-node ${typeClass}`}>
            <div className="custom-node-header">
                <div className="custom-node-icon">
                    {emoji}
                </div>
                <div className="custom-node-title">
                    {data.label}
                </div>
            </div>

            <Handle
                type="target"
                position={Position.Top}
                className="custom-handle"
            />
            <Handle
                type="source"
                position={Position.Bottom}
                className="custom-handle"
            />
        </div>
    );
}

// ===== Input Node =====
export function InputNode({ data }: { data: NodeData }) {
    return (
        <div className="custom-node custom-node-input">
            <div className="custom-node-header">
                <div className="custom-node-icon">{data.emoji || "🟢"}</div>
                <div className="custom-node-title">{data.label}</div>
            </div>

            <Handle
                type="source"
                position={Position.Bottom}
                id="output"
                className="custom-handle"
            />
        </div>
    );
}

// ===== Output Node =====
export function OutputNode({ data }: { data: NodeData }) {
    return (
        <div className="custom-node custom-node-output">
            <div className="custom-node-header">
                <div className="custom-node-icon">{data.emoji || "🔴"}</div>
                <div className="custom-node-title">{data.label}</div>
            </div>

            <Handle
                type="target"
                position={Position.Top}
                id="input"
                className="custom-handle"
            />
        </div>
    );
}

export default memo(DefaultNode);

export function AnimatedSVGEdge({
    id,
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
}: EdgeProps) {
    const [initialEdgePath] = getSmoothStepPath({
        sourceX,
        sourceY,
        targetX,
        targetY,
        sourcePosition,
        targetPosition,
    });

    let edgePath = initialEdgePath;

    // If the edge is almost vertical or horizontal, use a straight line
    const dx = Math.abs(targetX - sourceX);
    const dy = Math.abs(targetY - sourceY);
    if (dx < 1) {
        edgePath = `M${sourceX},${sourceY} L${sourceX + 1},${targetY}`;
    } else if (dy < 1) {
        edgePath = `M${sourceX},${sourceY} L${targetX},${sourceY + 1}`;
    }

    const dotCount = 4;
    const dotDur = 3.5;

    const dots = Array.from({ length: dotCount }, (_, i) => (
        <circle key={i} r="5" fill="url(#dotGradient)" opacity="0.8">
            <animateMotion
                dur={`${dotDur}s`}
                repeatCount="indefinite"
                begin={`${(i * dotDur) / dotCount}s`}
                path={edgePath}
            />
            <animate
                attributeName="r"
                values="5;7;5"
                dur={`${dotDur}s`}
                repeatCount="indefinite"
                begin={`${(i * dotDur) / dotCount}s`}
            />
        </circle>
    ));

    return (
        <>
            {/* Gradient Base Edge */}
            <BaseEdge
                id={id}
                path={edgePath}
                style={{
                    stroke: 'url(#gradientEdge)',
                    strokeWidth: 12,
                }}
            />

            {/* Dots */}
            {dots}

            {/* Flow shader line */}
            <path
                d={edgePath}
                fill="none"
                stroke="rgba(255,255,255,0.2)"
                strokeWidth={5}
                strokeDasharray="10 10"
            >
                <animate
                    attributeName="stroke-dashoffset"
                    from="20"
                    to="0"
                    dur="0.5s"
                    repeatCount="indefinite"
                />
            </path>

            {/* Gradient Color */}
            <defs>
                <linearGradient id="gradientEdge" gradientTransform="rotate(90)">
                    <stop offset="0%" stopColor="#4facfe" />
                    <stop offset="100%" stopColor="#00f2fe" />
                </linearGradient>

                <radialGradient id="dotGradient">
                    <stop offset="0%" stopColor="#fff" stopOpacity="1" />
                    <stop offset="50%" stopColor="#4facfe" stopOpacity="0.8" />
                    <stop offset="100%" stopColor="#00f2fe" stopOpacity="0.5" />
                </radialGradient>
            </defs>
        </>
    );
}