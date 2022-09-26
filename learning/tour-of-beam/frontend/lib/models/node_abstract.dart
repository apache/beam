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

import 'package:json_annotation/json_annotation.dart';

import '../enums.dart';
import 'group.dart';
import 'node_server.dart';
import 'unit.dart';

@JsonSerializable()
abstract class NodeModel {
  final NodeType type;
  final String title;
  const NodeModel({required this.type, required this.title});

  static List<NodeModel> nodesFromServer(List<Map<String, dynamic>> json) {
    return json
        .map<NodeServerModel>(NodeServerModel.fromJson)
        .map(_nodeFromServer)
        .toList();
  }

  static NodeModel _nodeFromServer(NodeServerModel nodeServer) {
    return nodeServer.type == NodeType.group
        ? GroupModel(
            title: nodeServer.group!.title,
            nodes: nodeServer.group!.nodes.map(_nodeFromServer).toList(),
          )
        : UnitModel(
            id: nodeServer.unit!.id,
            title: nodeServer.unit!.title,
          );
  }
}
