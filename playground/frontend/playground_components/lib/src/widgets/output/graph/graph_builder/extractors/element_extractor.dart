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

import '../../models/graph.dart';
import 'extractor_utils.dart';
import 'extractors.dart';

final RegExp kGraphElementRegExp = RegExp(r'''subgraph cluster_0''');
final RegExp kSubgraphElementRegExp = RegExp(r'''subgraph cluster_\d+''');
final RegExp kNodeElementRegExp = RegExp(r'''\d+\s\[label=".*"\]''');

final kElementsRegexps = [
  kGraphElementRegExp,
  kSubgraphElementRegExp,
  kNodeElementRegExp
];

class GraphElementExtractor implements Extractor<GraphElement> {
  @override
  GraphElement? extract(String line) {
    final lineWithoutSpaces = line.trim();
    if (kGraphElementRegExp.hasMatch(line)) {
      final match = kGraphElementRegExp.firstMatch(line);
      final start = match!.start;
      final end = match.end;
      return Graph(name: line.substring(start, end));
    }
    if (kSubgraphElementRegExp.hasMatch(line)) {
      final match = kSubgraphElementRegExp.firstMatch(line);
      final start = match!.start;
      final end = match.end;
      return Subgraph(depth: getDepth(line), name: line.substring(start, end));
    }
    if (kNodeElementRegExp.hasMatch(line)) {
      final name = extractNodeId(lineWithoutSpaces);
      final label = extractNodeLabel(lineWithoutSpaces);
      return Node(depth: getDepth(line), label: label, name: name);
    }
    return null;
  }

  @override
  bool check(String line) {
    return kElementsRegexps
        .where((element) => element.hasMatch(line))
        .isNotEmpty;
  }
}
