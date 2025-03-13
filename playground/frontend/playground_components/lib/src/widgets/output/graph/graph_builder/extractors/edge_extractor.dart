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
import 'extractors.dart';

final RegExp kEdgeRegExp = RegExp(r'''.+ -> .+''');
const kPrimaryEdgeStyle = 'solid';
const kEdgeSeparator = ' -> ';
const kStyleStart = '[';
const kStyleString = 'style';

class EdgeExtractor implements Extractor<Edge> {
  @override
  Edge? extract(String line) {
    final lineWithoutSpaces = line.trim();
    final styleStartIndex = lineWithoutSpaces.indexOf(kStyleStart);
    final endOfEdges =
    styleStartIndex > 0 ? styleStartIndex : lineWithoutSpaces.length;
    var edgesString = lineWithoutSpaces.substring(0, endOfEdges);
    if (edgesString.endsWith(';')) {
      edgesString = edgesString.substring(0, edgesString.length - 1);
    }
    final edges = edgesString.split(kEdgeSeparator);
    final isPrimary = lineWithoutSpaces.contains(kPrimaryEdgeStyle) ||
        !lineWithoutSpaces.contains(kStyleString);
    return Edge(
      startId: removeExtraSymbols(edges[0]),
      endId: removeExtraSymbols(edges[1]),
      isPrimary: isPrimary,
    );
  }

  removeExtraSymbols(String name) {
    final trimName = name.trim();
    if (trimName.isNotEmpty && trimName.startsWith('"')) {
      return trimName.substring(1, trimName.length - 1);
    }
    return trimName;
  }

  @override
  bool check(String line) {
    return kEdgeRegExp.hasMatch(line);
  }
}
