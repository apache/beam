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
import 'package:flutter/services.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';

class ClearOutputIntent extends BeamIntent {
  const ClearOutputIntent() : super(slug: 'intents.playground.clearOutput');
}

class NewExampleIntent extends BeamIntent {
  const NewExampleIntent() : super(slug: 'intents.playground.newExample');
}

final kClearOutputShortcut = BeamShortcut(
  keys: [
    LogicalKeyboardKeyExtension.metaOrControl,
    LogicalKeyboardKey.keyB,
  ],
  actionIntent: const ClearOutputIntent(),
  createAction: (BuildContext context) => CallbackAction(
    onInvoke: (_) => Provider.of<PlaygroundController>(
      context,
      listen: false,
    ).codeRunner.clearResult(),
  ),
);

final kNewExampleShortcut = BeamShortcut(
  keys: [
    LogicalKeyboardKeyExtension.metaOrControl,
    LogicalKeyboardKey.keyM,
  ],
  actionIntent: const NewExampleIntent(),
  createAction: (_) => CallbackAction(
    onInvoke: (_) => launchUrl(Uri.parse(BeamLinks.newExample)),
  ),
);

final List<BeamShortcut> globalShortcuts = [
  kClearOutputShortcut,
  kNewExampleShortcut,
];
