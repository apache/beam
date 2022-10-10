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

import 'dart:async';

import 'package:flutter/widgets.dart';

import '../models/content_tree.dart';
import '../repositories/client/client.dart';

class ContentTreeCache extends ChangeNotifier {
  final TobClient client;

  ContentTreeModel? _contentTree;
  Future<ContentTreeModel>? _future;

  ContentTreeCache({
    required this.client,
  });

  ContentTreeModel? getContentTree() {
    if (_future == null) {
      unawaited(_loadContentTree());
    }

    return _contentTree;
  }

  Future<ContentTreeModel?> _loadContentTree() async {
    _future = client.getContentTree();
    final result = await _future!;
    _contentTree = result;

    notifyListeners();
    return _contentTree;
  }
}
