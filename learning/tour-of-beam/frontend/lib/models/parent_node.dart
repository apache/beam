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

import 'package:collection/collection.dart';

import 'node.dart';
import 'unit.dart';

abstract class ParentNodeModel<T extends NodeModel> extends NodeModel {
  final List<T> nodes;

  const ParentNodeModel({
    required super.id,
    required super.parent,
    required super.title,
    required this.nodes,
  });

  @override
  List<UnitModel> getUnits() {
    return nodes
        .map((node) => node.getUnits())
        .expand((e) => e)
        .toList(growable: false);
  }

  @override
  NodeModel? getLastNodeFromBreadcrumbIds(List<String> breadcrumbIds) {
    final firstId = breadcrumbIds.firstOrNull;
    final child = nodes.firstWhereOrNull((node) => node.id == firstId);

    if (child == null) {
      return null;
    }

    return child.getLastNodeFromBreadcrumbIds(breadcrumbIds.sublist(1));
  }
}
