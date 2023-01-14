/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'package:flutter/material.dart';

enum NodeType {
  graph,
  subgraph,
  node,
}

Size getTextSize(String text, double fontSize) {
  final TextPainter textPainter = TextPainter(
    text: TextSpan(text: text, style: TextStyle(fontSize: fontSize)),
    maxLines: 1,
    textDirection: TextDirection.ltr,
  )..layout(minWidth: 0, maxWidth: double.infinity);
  return textPainter.size;
}

class GraphElement {
  String label;
  String name;
  final int depth;
  final NodeType type;
  GraphElement? parent;
  final List<GraphElement> child = [];

  GraphElement({
    required this.type,
    required this.depth,
    required this.name,
    this.label = '',
    this.parent,
  });
}

class Graph extends GraphElement {
  Graph({
    String label = '',
    String name = '',
    GraphElement? parent,
  }) : super(
          label: label,
          parent: parent,
          name: name,
          type: NodeType.graph,
          depth: 0,
        );
}

class Subgraph extends GraphElement {
  Subgraph({
    required int depth,
    String label = '',
    String name = '',
    GraphElement? parent,
  }) : super(
          label: label,
          parent: parent,
          name: name,
          type: NodeType.subgraph,
          depth: depth,
        );
}

class Edge {
  final String startId;
  final String endId;
  final bool isPrimary;

  Edge({
    required this.startId,
    required this.endId,
    required this.isPrimary,
  });

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Edge &&
          runtimeType == other.runtimeType &&
          startId == other.startId &&
          endId == other.endId;

  @override
  int get hashCode => startId.hashCode ^ endId.hashCode;
}

class Node extends GraphElement {
  final List<Edge> inEdges = [];
  final List<Edge> outEdges = [];

  Node({
    String label = '',
    required int depth,
    GraphElement? parent,
    String name = '',
  }) : super(
          label: label,
          parent: parent,
          name: name,
          type: NodeType.node,
          depth: depth,
        );
}
