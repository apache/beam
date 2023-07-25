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

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';

import 'example_share_tabs.dart';

/// Saves the current playground content when shown,
/// then presents a shareable link.
// TODO(alexeyinkin): Refactor code sharing, https://github.com/apache/beam/issues/24637
class SnippetSaveAndShareTabs extends StatefulWidget {
  final EventSnippetContext eventSnippetContext;
  final VoidCallback onError;
  final PlaygroundController playgroundController;
  final Sdk sdk;
  final TabController tabController;

  const SnippetSaveAndShareTabs({
    super.key,
    required this.eventSnippetContext,
    required this.onError,
    required this.playgroundController,
    required this.sdk,
    required this.tabController,
  });

  @override
  State<SnippetSaveAndShareTabs> createState() =>
      _SnippetSaveAndShareTabsState();
}

class _SnippetSaveAndShareTabsState extends State<SnippetSaveAndShareTabs> {
  Future<ExampleLoadingDescriptor>? _future;

  @override
  void initState() {
    super.initState();
    unawaited(_initSaving());
  }

  Future<void> _initSaving() async {
    try {
      _future = widget.playgroundController.saveSnippet();
      await _future;
    } on Exception catch (ex) {
      PlaygroundComponents.toastNotifier.addException(ex);
      widget.onError();
    }
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder(
      future: _future,
      builder: (context, snapshot) {
        final descriptor = snapshot.data;

        if (descriptor == null) {
          return const LoadingIndicator();
        }

        return ExampleShareTabs(
          descriptor: descriptor,
          eventSnippetContext: widget.eventSnippetContext,
          sdk: widget.sdk,
          tabController: widget.tabController,
        );
      },
    );
  }
}
