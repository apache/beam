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

import '../repositories/models/node.dart';
import '../repositories/models/node_type_enum.dart';
import 'group.dart';
import 'unit.dart';

/// Abstract NodeModel is used as the parent class of node models.
/// Nodes on server are based on composition,
/// because Golang doesn't support inheritance.
abstract class NodeModel {
  final String title;
  const NodeModel({required this.title});

  static List<NodeModel> nodesFromResponse(List json) {
    return json
        .cast<Map<String, dynamic>>()
        .map<NodeResponseModel>(NodeResponseModel.fromJson)
        .map(_nodeFromServer)
        .toList();
  }

  static NodeModel _nodeFromServer(NodeResponseModel node) {
    switch (node.type) {
      case NodeType.group:
        return GroupModel(
          title: node.group!.title,
          nodes: node.group!.nodes.map(_nodeFromServer).toList(),
        );
      case NodeType.unit:
        return UnitModel(
          id: node.unit!.id,
          title: node.unit!.title,
        );
    }
  }
}
