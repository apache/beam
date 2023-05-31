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
import 'package:provider/provider.dart';

import 'share_tabs/share_tabs.dart';
import 'share_tabs_headers.dart';

const _kTabsCount = 2;

class ShareDropdownBody extends StatefulWidget {
  final VoidCallback onError;
  final PlaygroundController playgroundController;

  const ShareDropdownBody({
    required this.onError,
    required this.playgroundController,
  });

  @override
  State<ShareDropdownBody> createState() => _ShareDropdownBodyState();
}

class _ShareDropdownBodyState extends State<ShareDropdownBody>
    with SingleTickerProviderStateMixin {
  late final EventSnippetContext _eventContext;
  late final tabController = TabController(vsync: this, length: _kTabsCount);

  @override
  void initState() {
    super.initState();
    // Saving the context before sharing to refer to the original snippet.
    _eventContext = widget.playgroundController.eventSnippetContext;
  }

  @override
  void dispose() {
    tabController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return ChangeNotifierProvider<PlaygroundController>.value(
      value: widget.playgroundController,
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          TabHeader(
            tabController: tabController,
            tabsWidget: ShareTabsHeaders(tabController: tabController),
          ),
          Expanded(
            child: ShareTabs(
              eventSnippetContext: _eventContext,
              tabController: tabController,
              onError: widget.onError,
            ),
          ),
        ],
      ),
    );
  }
}
