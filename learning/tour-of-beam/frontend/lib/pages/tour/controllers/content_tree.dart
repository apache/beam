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
import '../../../models/group.dart';
import '../../../models/node.dart';
import '../../../models/unit.dart';

class ContentTreeController extends ChangeNotifier {
  Sdk _sdk;
  List<String> _treeIds;
  NodeModel? _currentNode;
  final _contentTreeCache = GetIt.instance.get<ContentTreeCache>();
  final _expandedIds = <String>{};

  Set<String> get expandedIds => _expandedIds;

  ContentTreeController({
    required Sdk initialSdk,
    List<String> initialTreeIds = const [],
  })  : _sdk = initialSdk,
        _treeIds = initialTreeIds {
    _expandedIds.addAll(initialTreeIds);

    _contentTreeCache.addListener(_onContentTreeCacheChange);
    _onContentTreeCacheChange();
  }

  Sdk get sdk => _sdk;

  set sdk(Sdk newValue) {
    _sdk = newValue;
    notifyListeners();
  }

  List<String> get treeIds => _treeIds;
  NodeModel? get currentNode => _currentNode;

  void onNodePressed(NodeModel node) {
    if (node is GroupModel) {
      _onGroupPressed(node);
    } else if (node is UnitModel) {
      if (node != _currentNode) {
        _currentNode = node;
      }
    }

    if (_currentNode != null) {
      _treeIds = _getNodeAncestors(_currentNode!, [_currentNode!.id]);
    }
    notifyListeners();
  }

  void _onGroupPressed(GroupModel group) {
    if (_expandedIds.contains(group.id)) {
      _expandedIds.remove(group.id);
      notifyListeners();
    } else {
      _expandedIds.add(group.id);
      final groupFirstUnit = group.nodes.first;
      if (groupFirstUnit != _currentNode) {
        onNodePressed(groupFirstUnit);
      }
    }
  }

  void expandGroup(GroupModel group) {
    _expandedIds.add(group.id);
    notifyListeners();
  }

  void collapseGroup(GroupModel group) {
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
    final contentTree = _contentTreeCache.getContentTree(_sdk);
    if (contentTree == null) {
      return;
    }

    onNodePressed(
      contentTree.getNodeByTreeIds(_treeIds) ?? contentTree.getFirstUnit(),
    );

    notifyListeners();
  }

  @override
  void dispose() {
    _contentTreeCache.removeListener(_onContentTreeCacheChange);
    super.dispose();
  }
}
