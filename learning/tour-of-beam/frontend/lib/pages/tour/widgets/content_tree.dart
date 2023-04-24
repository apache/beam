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

import '../../../components/builders/content_tree.dart';
import '../controllers/content_tree.dart';
import 'content_tree_title.dart';
import 'module.dart';

// TODO(nausharipov): make it collapsible
class ContentTreeWidget extends StatelessWidget {
  final ContentTreeController controller;

  const ContentTreeWidget({
    required this.controller,
  });

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      width: 250,
      child: ContentTreeBuilder(
        sdk: controller.sdk,
        builder: (context, contentTree, child) {
          if (contentTree == null) {
            return Container();
          }

          return SingleChildScrollView(
            padding: const EdgeInsets.symmetric(
              horizontal: BeamSizes.size12,
            ),
            child: Column(
              children: [
                const ContentTreeTitleWidget(),
                ...contentTree.modules.map(
                  (module) => ModuleWidget(
                    module: module,
                    contentTreeController: controller,
                  ),
                ),
                const SizedBox(height: BeamSizes.size12),
              ],
            ),
          );
        },
      ),
    );
  }
}
