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
import 'package:playground/modules/output/components/output.dart';
import 'package:playground/pages/embedded_playground/components/embedded_actions.dart';
import 'package:playground/pages/embedded_playground/components/embedded_appbar_title.dart';
import 'package:playground/pages/embedded_playground/components/embedded_editor.dart';
import 'package:playground/pages/embedded_playground/components/embedded_split_view.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

const kActionsWidth = 300.0;
const kActionsHeight = 40.0;

class EmbeddedPlaygroundPage extends StatelessWidget {
  final bool isEditable;

  const EmbeddedPlaygroundPage({
    Key? key,
    required this.isEditable,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundState>(
      builder: (context, state, child) => Scaffold(
        appBar: AppBar(
          automaticallyImplyLeading: false,
          title: const EmbeddedAppBarTitle(),
          actions: const [EmbeddedActions()],
        ),
        body: EmbeddedSplitView(
          first: EmbeddedEditor(isEditable: isEditable),
          second: Container(
            color: Theme.of(context).backgroundColor,
            child: Output(
              isEmbedded: true,
              showGraph: state.graphAvailable,
              key: ValueKey(state.selectedExample?.path ?? state.sdk.toString())
            ),
          ),
        ),
      ),
    );
  }
}
