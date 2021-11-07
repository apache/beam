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
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/editor/components/editor_textarea.dart';
import 'package:playground/modules/editor/components/run_button.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

class CodeTextAreaWrapper extends StatelessWidget {
  const CodeTextAreaWrapper({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundState>(builder: (context, state, child) {
      if (state.result?.errorMessage?.isNotEmpty ?? false) {
        WidgetsBinding.instance?.addPostFrameCallback((_) {
          _handleError(context, state);
        });
      }
      return Stack(
        children: [
          Positioned.fill(
            child: EditorTextArea(
              key: ValueKey(EditorKeyObject(
                state.sdk,
                state.selectedExample,
                state.resetKey,
              )),
              example: state.selectedExample,
              sdk: state.sdk,
              onSourceChange: state.setSource,
            ),
          ),
          Positioned(
            right: kXlSpacing,
            top: kXlSpacing,
            width: kRunButtonWidth,
            height: kRunButtonHeight,
            child: RunButton(
              isRunning: state.isCodeRunning,
              runCode: state.runCode,
            ),
          ),
        ],
      );
    });
  }

  _handleError(BuildContext context, PlaygroundState state) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(state.result?.errorMessage ?? '')),
    );
    state.resetError();
  }
}

class EditorKeyObject {
  final SDK sdk;
  final ExampleModel? example;
  final DateTime? resetKey;

  const EditorKeyObject(this.sdk, this.example, this.resetKey);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is EditorKeyObject &&
          runtimeType == other.runtimeType &&
          sdk == other.sdk &&
          example == other.example &&
          resetKey == other.resetKey;

  @override
  int get hashCode => sdk.hashCode ^ example.hashCode ^ resetKey.hashCode;
}
