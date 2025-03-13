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

import '../../../models/module.dart';
import '../controllers/content_tree.dart';
import 'module_title.dart';
import 'parent_node.dart';

class ModuleWidget extends StatelessWidget {
  final ModuleModel module;
  final ContentTreeController contentTreeController;

  ModuleWidget({
    required this.module,
    required this.contentTreeController,
  }) : super(key: Key(module.id));

  @override
  Widget build(BuildContext context) {
    return ParentNodeWidget(
      contentTreeController: contentTreeController,
      node: module,
      title: ModuleTitleWidget(
        module: module,
        onTap: () => contentTreeController.onNodePressed(module),
      ),
    );
  }
}
