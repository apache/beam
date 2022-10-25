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

class ContentTreeController extends ChangeNotifier {
  String _sdkId;
  List<String> _treeIds;
  NodeModel? _currentNode;
  final _contentTreeCache = GetIt.instance.get<ContentTreeCache>();
  final expandedIds = <String>{};

  ContentTreeController({
    required String initialSdkId,
    List<String> initialTreeIds = const [],
  })  : _sdkId = initialSdkId,
        _treeIds = initialTreeIds {
    expandedIds.addAll(initialTreeIds);
    _contentTreeCache.addListener(_onContentTreeCacheChange);
    _onContentTreeCacheChange();
  }

  Sdk get sdk => Sdk.parseOrCreate(_sdkId);
  String get sdkId => _sdkId;
  List<String> get treeIds => _treeIds;
  NodeModel? get currentNode => _currentNode;

  void onNodeTap(NodeModel node) {
    if (node == _currentNode) {
      return;
    }

    expandedIds.add(node.id);

    if (node is GroupModel) {
      _currentNode = node.nodes.first;
    } else {
      _currentNode = node;
    }
    if (_currentNode != null) {
      _treeIds = _getNodeAncestors(_currentNode!, [_currentNode!.id]);
    }
    notifyListeners();
  }

  List<String> _getNodeAncestors(NodeModel node, List<String> ancestors) {
    if (node.parent != null) {
      ancestors.add(node.parent!.id);
      return _getNodeAncestors(node.parent!, ancestors);
    } else {
      return ancestors.reversed.toList();
    }
  }

  void _onContentTreeCacheChange() {
    final contentTree = _contentTreeCache.getContentTree(_sdkId);
    if (contentTree == null) {
      return;
    }

    _currentNode = contentTree.getNodeByTreeIds(_treeIds);
    notifyListeners();
  }

  @override
  void dispose() {
    _contentTreeCache.removeListener(_onContentTreeCacheChange);
    super.dispose();
  }
}
