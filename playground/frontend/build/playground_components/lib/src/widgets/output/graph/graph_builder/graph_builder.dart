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

import 'dart:convert';

import 'package:flutter/widgets.dart' as widgets;
import 'package:playground_components/playground_components.dart';

import '../models/graph.dart';
import '../models/table_cell.dart';
import 'extractors/edge_extractor.dart';
import 'extractors/element_extractor.dart';
import 'extractors/label_extractor.dart';
import 'painters/edge_painter.dart';
import 'painters/graph_painter.dart';
import 'painters/node_painter.dart';

final kGraphElementExtractor = GraphElementExtractor();
final kLabelExtractor = LabelExtractor();
final kEdgeExtractor = EdgeExtractor();

abstract class GraphBuilder {
  final List<GraphElement> elements = [];
  final List<Edge> edges = [];
  final Map<String, GraphElement> elementsMap = {};

  // TODO(alexeyinkin): Use this as the source of truth
  //  of whether a graph is available for an SDK,
  //  https://github.com/apache/beam/issues/23251
  static final _graphBuilderFactoriesBySdk = {
    Sdk.java: JavaGraphBuilder.new,
    Sdk.python: PythonGraphBuilder.new,
  };

  static GraphBuilder? parseDot(String dot, Sdk sdk) {
    LineSplitter ls = const LineSplitter();
    List<String> lines = ls.convert(dot);
    final builder = _graphBuilderFactoriesBySdk[sdk]?.call();

    if (builder == null) {
      return null;
    }

    for (var line in lines) {
      builder.parseNextLine(line);
    }
    builder.finish();
    return builder;
  }

  void parseNextLine(String line);

  GraphPainter getPainter(widgets.Axis direction) {
    final List<Node> nodeElements = elements
        .where((element) => element.type == NodeType.node)
        .toList()
        .cast<Node>();
    // nodes without incoming edges
    final List<Node> startNodes =
        nodeElements.where((element) => element.inEdges.isEmpty).toList();

    final List<Node> nodesInTopologicalOrder =
        sortElementsInTopologicalOrder(startNodes);

    final Map<String, int> nodeToColumnsMap = determineNodesColumns(
        nodeElements, startNodes, nodesInTopologicalOrder);
    final Map<String, TableCell<int>> nodeToCellMap = getNodeToCellMap(
      nodesInTopologicalOrder,
      nodeToColumnsMap,
    );

    return GraphPainter(
      direction: direction,
      elementsPainter: elements
          .where((element) => element.type == NodeType.node)
          .map<NodeElementPainter>((element) {
        final cell = nodeToCellMap[element.name]!;
        final row =
            direction == widgets.Axis.horizontal ? cell.row : cell.column;
        final column =
            direction == widgets.Axis.horizontal ? cell.column : cell.row;
        return NodeElementPainter(
          element: element as Node,
          row: row,
          column: column,
        );
      }).toList(),
      edges: edges.map((e) => EdgePainter(e)).toList(),
    );
  }

  /// Use Kahn's algorithm to sort elements in topological order
  sortElementsInTopologicalOrder(List<Node> startNodes) {
    final List<Node> startNodesCopy = [...startNodes];
    final List<Node> topologicalOrder = [];
    final Set<Edge> visitedEdges = {};
    while (startNodesCopy.isNotEmpty) {
      final currentNode = startNodesCopy.removeAt(0);
      topologicalOrder.add(currentNode);
      for (var edge in currentNode.outEdges) {
        final edgeNode = elementsMap[edge.endId] as Node;
        visitedEdges.add(edge);
        final hasInEdges =
            edgeNode.inEdges.where((e) => !visitedEdges.contains(e)).isNotEmpty;
        if (!hasInEdges) {
          startNodesCopy.add(edgeNode);
        }
      }
    }
    return topologicalOrder;
  }

  Map<String, int> determineNodesColumns(List<Node> nodeElements,
      List<Node> startNodes, List<Node> nodesInTopologicalOrder) {
    final Map<String, int> nodeToColumnsMap = {};
    for (var element in nodeElements) {
      nodeToColumnsMap[element.name] = -1;
    }
    nodeToColumnsMap[nodesInTopologicalOrder.first.name] = 0;
    final nodeInTopologicalOrderCopy = [...nodesInTopologicalOrder];
    while (nodeInTopologicalOrderCopy.isNotEmpty) {
      Node node = nodeInTopologicalOrderCopy.removeAt(0);
      // Update distances of all adjacent vertices ;
      if (nodeToColumnsMap[node.name] != -1) {
        for (var edge in node.outEdges) {
          if (nodeToColumnsMap[edge.endId]! <
              nodeToColumnsMap[edge.startId]! + 1) {
            nodeToColumnsMap[edge.endId] = nodeToColumnsMap[edge.startId]! + 1;
          }
        }
      }
    }
    for (var element in nodeElements) {
      final column = nodeToColumnsMap[element.name]!;
      if (column < 0) {
        nodeToColumnsMap[element.name] = 0;
      }
    }
    return nodeToColumnsMap;
  }

  Map<String, TableCell<int>> getNodeToCellMap(
      List<Node> nodesInTopologicalOrder, Map<String, int> nodeToColumnsMap) {
    final Map<String, TableCell<int>> nodeToCellMap = {};
    final Map<int, int> rowToColumn = {};

    for (var node in nodesInTopologicalOrder) {
      final column = nodeToColumnsMap[node.name]!;
      final row = (rowToColumn[column] ?? -1) + 1;
      rowToColumn[column] = row;
      nodeToCellMap[node.name] = TableCell(row: row, column: column);
    }
    return nodeToCellMap;
  }

  void finish() {
    for (var edge in edges) {
      (elementsMap[edge.startId] as Node).outEdges.add(edge);
      (elementsMap[edge.endId] as Node).inEdges.add(edge);
    }
  }
}

class JavaGraphBuilder extends GraphBuilder {
  final List<GraphElement> parentElements = [];
  GraphElement? lastElement;

  @override
  void parseNextLine(String line) {
    try {
      if (kGraphElementExtractor.check(line)) {
        final element = kGraphElementExtractor.extract(line);
        if (element == null) {
          return;
        }
        elements.add(element);
        setParent(element);
        if (element.type != NodeType.node) {
          parentElements.add(element);
          lastElement = element;
        }
        elementsMap[element.name] = element;
      } else if (kLabelExtractor.check(line)) {
        final label = kLabelExtractor.extract(line);
        lastElement?.label = label ?? '';
      } else if (kEdgeExtractor.check(line)) {
        final edge = kEdgeExtractor.extract(line);
        if (edge != null) {
          edges.add(edge);
        }
      }
    } catch (e) {
      print(e);
    }
  }

  setParent(GraphElement element) {
    final lastParent = parentElements.isNotEmpty ? parentElements.last : null;
    if (lastParent != null) {
      if (lastParent.depth >= element.depth) {
        while (parentElements.isNotEmpty &&
            parentElements.last.depth >= element.depth) {
          parentElements.removeLast();
        }
      }
      final prevParent = parentElements.isNotEmpty ? parentElements.last : null;
      element.parent = prevParent;
      prevParent?.child.add(element);
    }
  }
}

const kPythonDefaultCollectionLabel = 'pcoll';
const kPythonCollectionLabel = 'PCollection';

class PythonGraphBuilder extends GraphBuilder {
  @override
  void parseNextLine(String line) {
    if (kEdgeExtractor.check(line)) {
      final edge = kEdgeExtractor.extract(line);
      if (edge != null) {
        createNodes(edge);
        edges.add(edge);
      }
    }
  }

  createNodes(Edge edge) {
    createNodeIfNotExists(edge.startId);
    createNodeIfNotExists(edge.endId);
  }

  createNodeIfNotExists(String name) {
    if (elementsMap[name] != null) {
      return;
    }
    final label = name.replaceFirst(
        kPythonDefaultCollectionLabel, kPythonCollectionLabel);
    Node node = Node(label: label, depth: 1, name: name);
    elementsMap[name] = node;
    elements.add(node);
  }
}
