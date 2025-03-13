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

import '../repositories/models/group.dart';
import 'node.dart';
import 'parent_node.dart';

class GroupModel extends ParentNodeModel {
  const GroupModel({
    required super.id,
    required super.nodes,
    required super.parent,
    required super.title,
  });

  factory GroupModel.fromResponse(
    GroupResponseModel groupResponse,
    ParentNodeModel parent,
  ) {
    final group = GroupModel(
      id: groupResponse.id,
      nodes: [],
      parent: parent,
      title: groupResponse.title,
    );

    group.nodes.addAll(
      groupResponse.nodes.map<NodeModel>(
        (node) => NodeModel.fromResponse(node, group),
      ),
    );

    return group;
  }
}
