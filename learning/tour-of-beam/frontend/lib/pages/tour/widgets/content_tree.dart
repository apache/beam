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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../../components/builders/content_tree.dart';
import '../../../state.dart';
import '../controllers/content_tree.dart';
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
      child: AnimatedBuilder(
        animation: GetIt.instance.get<AppNotifier>(),
        builder: (context, child) => ContentTreeBuilder(
          sdk: GetIt.instance.get<AppNotifier>().sdk,
          builder: (context, contentTree, child) {
            if (contentTree == null) {
              return const Center(child: CircularProgressIndicator());
            }

            return SingleChildScrollView(
              padding: const EdgeInsets.symmetric(
                horizontal: BeamSizes.size12,
              ),
              child: Column(
                children: [
                  const _Title(),
                  ...contentTree.nodes.map(
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
      ),
    );
  }
}

class _Title extends StatelessWidget {
  const _Title();

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: BeamSizes.size12),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(
            'pages.tour.summaryTitle',
            style: Theme.of(context).textTheme.headlineLarge,
          ).tr(),
        ],
      ),
    );
  }
}
