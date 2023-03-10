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
import 'package:playground/constants/colors.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_option_controller.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_input.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_separator.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_form.dart';
import 'package:playground_components/playground_components.dart';

const kOptionsTabIndex = 0;
const kRawTabIndex = 1;

class PipelineOptionsDropdownBody extends StatefulWidget {
  final String pipelineOptions;
  final void Function(String) setPipelineOptions;
  final void Function() close;

  PipelineOptionsDropdownBody({
    required this.pipelineOptions,
    required this.setPipelineOptions,
    required this.close,
  }) : super(key: ValueKey(pipelineOptions));

  @override
  State<PipelineOptionsDropdownBody> createState() =>
      _PipelineOptionsDropdownBodyState();
}

class _PipelineOptionsDropdownBodyState
    extends State<PipelineOptionsDropdownBody>
    with SingleTickerProviderStateMixin {
  late final TabController tabController;
  final TextEditingController pipelineOptionsController =
      TextEditingController();
  List<PipelineOptionController> pipelineOptionsList = [];
  int selectedTab = kOptionsTabIndex;
  bool showError = false;

  @override
  void initState() {
    tabController = TabController(vsync: this, length: 2);
    tabController.addListener(onTabChange);
    pipelineOptionsController.text = widget.pipelineOptions;
    pipelineOptionsList = _pipelineOptionsMapToList(widget.pipelineOptions);
    if (pipelineOptionsList.isEmpty) {
      pipelineOptionsList = [PipelineOptionController()];
    }
    super.initState();
  }

  @override
  void dispose() {
    tabController.removeListener(onTabChange);
    tabController.dispose();
    pipelineOptionsController.dispose();

    for (final controller in pipelineOptionsList) {
      controller.dispose();
    }

    super.dispose();
  }

  onTabChange() {
    setState(() {
      selectedTab = tabController.index;
    });
    if (tabController.index == kRawTabIndex) {
      _updateRawValue();
    } else {
      _updateFormValue();
    }
  }

  onDelete(int index) {
    setState(() {
      pipelineOptionsList.removeAt(index);
    });
  }

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;
    return Column(
      children: [
        TabBar(
          controller: tabController,
          tabs: <Widget>[
            Tab(text: appLocale.optionsPipelineOptions),
            Tab(text: appLocale.rawPipelineOptions),
          ],
        ),
        const PipelineOptionsDropdownSeparator(),
        Expanded(
          child: Padding(
            padding: const EdgeInsets.all(kXlSpacing),
            child: TabBarView(
              controller: tabController,
              physics: const NeverScrollableScrollPhysics(),
              children: <Widget>[
                PipelineOptionsForm(
                  options: pipelineOptionsList,
                  onDelete: onDelete,
                ),
                PipelineOptionsDropdownInput(
                  controller: pipelineOptionsController,
                ),
              ],
            ),
          ),
        ),
        const PipelineOptionsDropdownSeparator(),
        Padding(
          padding: const EdgeInsets.all(kXlSpacing),
          child: Row(
            crossAxisAlignment: CrossAxisAlignment.center,
            children: [
              SizedBox(
                height: kButtonHeight,
                child: ElevatedButton(
                  child: Text(appLocale.saveAndClose),
                  onPressed: () => _save(context),
                ),
              ),
              const SizedBox(width: kLgSpacing),
              if (selectedTab == kOptionsTabIndex)
                SizedBox(
                  height: kButtonHeight,
                  child: OutlinedButton(
                    child: Text(appLocale.addPipelineOptionParameter),
                    onPressed: () => setState(() {
                      pipelineOptionsList.add(PipelineOptionController());
                    }),
                  ),
                ),
              if (showError && selectedTab == kRawTabIndex)
                Flexible(
                  child: Text(
                    appLocale.pipelineOptionsError,
                    style: Theme.of(context)
                        .textTheme
                        .caption!
                        .copyWith(color: kErrorNotificationColor),
                    softWrap: true,
                  ),
                ),
            ],
          ),
        )
      ],
    );
  }

  Map<String, String> get pipelineOptionsListValue {
    final notEmptyOptions = pipelineOptionsList
        .where((controller) =>
            controller.nameController.text.isNotEmpty &&
            controller.valueController.text.isNotEmpty)
        .toList();
    return {
      for (final controller in notEmptyOptions)
        controller.nameController.text: controller.valueController.text
    };
  }

  String get pipelineOptionsValue {
    if (selectedTab == kRawTabIndex) {
      return pipelineOptionsController.text;
    }
    return pipelineOptionsToString(pipelineOptionsListValue);
  }

  _save(BuildContext context) {
    if (selectedTab == kRawTabIndex && !_isPipelineOptionsTextValid()) {
      setState(() {
        showError = true;
      });
      return;
    }
    widget.setPipelineOptions(pipelineOptionsValue);
    widget.close();
  }

  bool _isPipelineOptionsTextValid() {
    final options = pipelineOptionsController.text;
    final parsedOptions = parsePipelineOptions(options);
    return options.isEmpty || (parsedOptions != null);
  }

  _updateRawValue() {
    if (pipelineOptionsListValue.isNotEmpty) {
      pipelineOptionsController.text =
          pipelineOptionsToString(pipelineOptionsListValue);
    }
  }

  _updateFormValue() {
    final parsedOptions =
        _pipelineOptionsMapToList(pipelineOptionsController.text);
    if (parsedOptions.isNotEmpty) {
      setState(() {
        pipelineOptionsList = parsedOptions;
      });
    }
  }

  List<PipelineOptionController> _pipelineOptionsMapToList(
      String pipelineOptions) {
    return parsePipelineOptions(pipelineOptions)
            ?.entries
            .map((e) => PipelineOptionController(name: e.key, value: e.value))
            .toList() ??
        [];
  }
}
