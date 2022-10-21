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
import 'package:playground_components/playground_components.dart';

import '../../../components/expansion_tile_wrapper.dart';
import '../../../models/group.dart';
import '../controllers/content_tree.dart';
import 'group_nodes.dart';
import 'group_title.dart';

class GroupWidget extends StatefulWidget {
  final GroupModel group;
  final ContentTreeController contentTreeController;

  const GroupWidget({
    required this.group,
    required this.contentTreeController,
  });

  @override
  State<GroupWidget> createState() => _GroupWidgetState();
}

class _GroupWidgetState extends State<GroupWidget> {
  bool _isExpanded = false;

  @override
  Widget build(BuildContext context) {
    return ExpansionTileWrapper(
      ExpansionTile(
        key: Key('${widget.group.id}$_isExpanded'),
        initiallyExpanded: _isExpanded,
        tilePadding: EdgeInsets.zero,
        title: GroupTitleWidget(
          group: widget.group,
          onTap: () {
            widget.contentTreeController.onNodeTap(widget.group);
            setState(() {
              _isExpanded = !_isExpanded;
            });
          },
        ),
        childrenPadding: const EdgeInsets.only(
          left: BeamSizes.size24,
        ),
        children: [
          GroupNodesWidget(
            nodes: widget.group.nodes,
            contentTreeController: widget.contentTreeController,
          ),
        ],
      ),
    );
  }
}
