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

<<<<<<<< HEAD:playground/frontend/playground_components_dev/lib/src/finder.dart
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';

extension FinderExtension on Finder {
  // TODO(alexeyinkin): Push to Flutter or wait for them to make their own, https://github.com/flutter/flutter/issues/117675
  Finder and(Finder another) {
    return _AndFinder(this, another);
  }
}

class _AndFinder extends ChainedFinder {
  _AndFinder(super.parent, this.another);

  final Finder another;

  @override
  String get description => '${parent.description} AND ${another.description}';

  @override
  Iterable<Element> filter(Iterable<Element> parentCandidates) {
    return another.apply(parentCandidates);
  }
========
import '../repositories/models/get_content_tree_response.dart';
import 'module.dart';
import 'node.dart';
import 'parent_node.dart';

class ContentTreeModel extends ParentNodeModel {
  final List<ModuleModel> modules;

  String get sdkId => id;

  @override
  List<NodeModel> get nodes => modules;

  const ContentTreeModel({
    required super.id,
    required this.modules,
  }) : super(
          parent: null,
          title: '',
          nodes: modules,
        );

  ContentTreeModel.fromResponse(GetContentTreeResponse response)
      : this(
          id: response.sdkId,
          modules: response.modules
              .map(ModuleModel.fromResponse)
              .toList(growable: false),
        );
>>>>>>>> 66796913c3 (Merge from oss 2.45.0):learning/tour-of-beam/frontend/lib/models/content_tree.dart
}
