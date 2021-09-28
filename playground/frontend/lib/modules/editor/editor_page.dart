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
import 'package:provider/provider.dart';
import 'package:playground/modules/editor/components/sdk_selector/sdk_selector.dart';
import 'package:playground/modules/editor/state/editor_state.dart';
import 'package:playground/components/logo/logo_component.dart';
import 'package:playground/modules/editor/components/code_textarea/code_textarea.dart';
import 'package:playground/modules/editor/components/output_area/output_area.dart';

class EditorPage extends StatelessWidget {
  const EditorPage({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return ChangeNotifierProvider<EditorState>(
      create: (context) => EditorState(),
      child: Scaffold(
        appBar: AppBar(
          title: Wrap(
            crossAxisAlignment: WrapCrossAlignment.center,
            spacing: 16.0,
            children: const [
              LogoComponent(),
              SDKSelector(),
            ],
          ),
        ),
        body: Column(
          children: [
            const Expanded(child: CodeTextArea()),
            Container(height: 16.0, color: Theme.of(context).backgroundColor),
            const Expanded(child: OutputArea()),
          ],
        ),
      ),
    );
  }
}
