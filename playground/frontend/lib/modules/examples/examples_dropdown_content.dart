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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

import '../../constants/sizes.dart';
import 'components/example_list/example_list.dart';
import 'components/filter/filter.dart';
import 'components/search_field/search_field.dart';

class ExamplesDropdownContent extends StatefulWidget {
  final VoidCallback onSelected;
  final PlaygroundController playgroundController;

  const ExamplesDropdownContent({
    required this.onSelected,
    required this.playgroundController,
  });

  @override
  State<ExamplesDropdownContent> createState() =>
      _ExamplesDropdownContentState();
}

class _ExamplesDropdownContentState extends State<ExamplesDropdownContent> {
  final _searchController = TextEditingController();

  @override
  Widget build(BuildContext context) {
    final exampleCache = widget.playgroundController.exampleCache;

    return AnimatedBuilder(
      animation: exampleCache,
      builder: (context, child) {
        switch (exampleCache.catalogStatus) {
          case LoadingStatus.error:
            return const LoadingErrorWidget();

          case LoadingStatus.loading:
            return const LoadingIndicator();

          case LoadingStatus.done:
            return _buildLoaded();
        }
      },
    );
  }

  Widget _buildLoaded() {
    return Column(
      children: [
        SearchField(controller: _searchController),
        const ExamplesFilter(),
        ExampleList(
          onSelected: widget.onSelected,
          selectedExample: widget.playgroundController.selectedExample,
        ),
        const BeamDivider(),
        SizedBox(
          width: double.infinity,
          child: TextButton(
            child: Padding(
              padding: const EdgeInsets.all(kXlSpacing),
              child: Align(
                alignment: Alignment.centerLeft,
                child: Text(
                  AppLocalizations.of(context)!.addExample,
                  style: TextStyle(color: Theme.of(context).primaryColor),
                ),
              ),
            ),
            onPressed: () => launchUrl(Uri.parse(BeamLinks.newExample)),
          ),
        ),
      ],
    );
  }

  @override
  void dispose() {
    _searchController.dispose();
    super.dispose();
  }
}
