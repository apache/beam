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

// TODO(alexeyinkin): In future convert all dialogs to this one.
class BeamDialog extends StatelessWidget {
  static const _padding = 40.0;

  const BeamDialog({
    required this.child,
    this.actions = const [],
    this.title,
  });

  final List<Widget> actions;
  final Widget child;
  final Widget? title;

  static Future<void> show({
    required Widget child,
    required BuildContext context,
    Widget? title,
    List<Widget> actions = const [],
  }) async {
    await showDialog<void>(
      context: context,
      builder: (BuildContext context) => BeamDialog(
        actions: actions,
        title: title,
        child: child,
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      actions: actions,
      actionsPadding: const EdgeInsets.only(
        bottom: _padding,
        right: _padding,
      ),
      content: child,
      contentPadding: const EdgeInsets.all(_padding),
      title: title,
      titlePadding: const EdgeInsets.only(
        top: _padding,
        left: _padding,
      ),
    );
  }
}
