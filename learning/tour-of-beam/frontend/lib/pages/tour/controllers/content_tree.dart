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

import 'package:flutter/widgets.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../../cache/content_tree.dart';

import '../../../models/node.dart';
import '../../../models/parent_node.dart';
import '../../../models/unit.dart';
import '../../../state.dart';

class ContentTreeController extends ChangeNotifier {
  final Sdk initialSdk;
  List<String> _treeIds;
  NodeModel? _currentNode;
  final _contentTreeCache = GetIt.instance.get<ContentTreeCache>();
  final _expandedIds = <String>{};

  Set<String> get expandedIds => _expandedIds;

  ContentTreeController({
    required this.initialSdk,
    List<String> initialTreeIds = const [],
  }) : _treeIds = initialTreeIds {
    _expandedIds.addAll(initialTreeIds);

    _contentTreeCache.addListener(_onContentTreeCacheChange);
    _onContentTreeCacheChange();
  }

  Sdk get sdk => GetIt.instance.get<AppNotifier>().sdk ?? initialSdk;
  List<String> get treeIds => _treeIds;
  NodeModel? get currentNode => _currentNode;

  void onNodePressed(NodeModel node) {
    _toggleNode(node);
    notifyListeners();
  }

  void _toggleNode(NodeModel node) {
    if (node is ParentNodeModel) {
      _onParentNodePressed(node);
    } else if (node is UnitModel) {
      if (node != _currentNode) {
        _currentNode = node;
      }
    }

    if (_currentNode != null) {
      _treeIds = _getNodeAncestors(_currentNode!, [_currentNode!.id]);
    }
  }

  void _onParentNodePressed(ParentNodeModel node) {
    if (_expandedIds.contains(node.id)) {
      _expandedIds.remove(node.id);
      notifyListeners();
    } else {
      _expandedIds.add(node.id);

      final firstChildNode = node.nodes.first;
      if (firstChildNode != _currentNode) {
        _toggleNode(firstChildNode);
      }
    }
  }

  void expandParentNode(ParentNodeModel group) {
    _expandedIds.add(group.id);
    notifyListeners();
  }

  void collapseParentNode(ParentNodeModel group) {
    _expandedIds.remove(group.id);
    notifyListeners();
  }

  List<String> _getNodeAncestors(NodeModel node, List<String> ancestorIds) {
    if (node.parent != null) {
      return _getNodeAncestors(
        node.parent!,
        [...ancestorIds, node.parent!.id],
      );
    }
    return ancestorIds.reversed.toList();
  }

  void _onContentTreeCacheChange() {
    final contentTree = _contentTreeCache.getContentTree(sdk);
    if (contentTree == null) {
      return;
    }

    _toggleNode(
      contentTree.getNodeByTreeIds(_treeIds) ?? contentTree.modules.first,
    );

    notifyListeners();
  }

  @override
  void dispose() {
    _contentTreeCache.removeListener(_onContentTreeCacheChange);
    super.dispose();
  }
}
