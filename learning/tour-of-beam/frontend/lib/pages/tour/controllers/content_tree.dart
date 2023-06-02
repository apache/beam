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

import '../../../cache/content_tree.dart';

import '../../../models/node.dart';
import '../../../models/parent_node.dart';
import '../../../models/unit.dart';
import '../../../state.dart';

class ContentTreeController extends ChangeNotifier {
  List<String> _breadcrumbIds;
  NodeModel? _currentNode;
  final _contentTreeCache = GetIt.instance.get<ContentTreeCache>();
  final _expandedIds = <String>{};

  final _units = <UnitModel>[];
  int _currentUnitIndex = 0;

  Set<String> get expandedIds => _expandedIds;

  ContentTreeController({
    List<String> initialBreadcrumbIds = const [],
  }) : _breadcrumbIds = initialBreadcrumbIds {
    _expandedIds.addAll(initialBreadcrumbIds);

    _contentTreeCache.addListener(_onContentTreeCacheChange);
    _onContentTreeCacheChange();
  }

  List<String> get breadcrumbIds => _breadcrumbIds;
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
        _setUnit(node);
      }
    }
  }

  void _setUnit(UnitModel unit) {
    _currentNode = unit;
    _currentUnitIndex = _getCurrentUnitIndex();
    _breadcrumbIds = _getNodeAncestors(_currentNode!, [_currentNode!.id]);
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
    final contentTree = _contentTreeCache.getContentTree(
      GetIt.instance.get<AppNotifier>().sdk,
    );
    if (contentTree == null) {
      return;
    }

    _units.clear();
    _units.addAll(contentTree.getUnits());

    _toggleNode(
      contentTree.getLastNodeFromBreadcrumbIds(_breadcrumbIds) ??
          contentTree.nodes.first,
    );

    notifyListeners();
  }

  bool hasPreviousUnit() {
    return _currentUnitIndex > 0;
  }

  bool hasNextUnit() {
    return _currentUnitIndex < _units.length - 1;
  }

  void openPreviousUnit() {
    final previousUnit = _units[_currentUnitIndex - 1];
    _navigateToUnit(previousUnit);
  }

  void openNextUnit() {
    final nextUnit = _units[_currentUnitIndex + 1];
    _navigateToUnit(nextUnit);
  }

  int _getCurrentUnitIndex() {
    return _units.indexWhere(
      (unit) => unit.id == _currentNode?.id,
    );
  }

  void _navigateToUnit(UnitModel unit) {
    onNodePressed(unit);
    _expandedIds.addAll(_breadcrumbIds);
  }

  @override
  void dispose() {
    _contentTreeCache.removeListener(_onContentTreeCacheChange);
    super.dispose();
  }
}
