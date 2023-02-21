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

import '../../controllers/playground_controller.dart';
import '../../enums/unread_entry.dart';
import '../unread/builder.dart';
import 'output_tab.dart';

class GraphTab extends StatelessWidget {
  const GraphTab({
    required this.playgroundController,
  });

  final PlaygroundController playgroundController;

  @override
  Widget build(BuildContext context) {
    return UnreadBuilder(
      controller: playgroundController.codeRunner.unreadController,
      unreadKey: UnreadEntryEnum.graph,
      builder: (context, isUnread) {
        return OutputTab(
          isUnread: isUnread,
          title: 'widgets.output.graph'.tr(),
        );
      },
    );
  }
}
