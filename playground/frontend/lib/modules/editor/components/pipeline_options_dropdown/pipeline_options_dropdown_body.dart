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
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_option_model.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_input.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_separator.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_form.dart';
import 'package:playground/modules/editor/parsers/run_options_parser.dart';

const kOptionsTabIndex = 0;
const kRawTabIndex = 1;

final kDefaultOption = [PipelineOptionController()];

class PipelineOptionsDropdownBody extends StatefulWidget {
  final String pipelineOptions;
  final Function(String) setPipelineOptions;
  final Function close;

  const PipelineOptionsDropdownBody({
    Key? key,
    required this.pipelineOptions,
    required this.setPipelineOptions,
    required this.close,
  }) : super(key: key);

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
  List<PipelineOptionController> pipelineOptionsList = kDefaultOption;
  int selectedTab = kOptionsTabIndex;

  @override
  void initState() {
    tabController = TabController(vsync: this, length: 2);
    tabController.addListener(onTabChange);
    pipelineOptionsController.text = widget.pipelineOptions;
    pipelineOptionsList = _pipelineOptionsMapToList(widget.pipelineOptions);
    if (pipelineOptionsList.isEmpty) {
      pipelineOptionsList = kDefaultOption;
    }
    super.initState();
  }

  @override
  void dispose() {
    tabController.removeListener(onTabChange);
    tabController.dispose();
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
            children: [
              SizedBox(
                height: kButtonHeight,
                child: ElevatedButton(
                  child: Text(appLocale.saveAndClose),
                  onPressed: () {
                    widget.setPipelineOptions(pipelineOptionsValue);
                    widget.close();
                  },
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
            ],
          ),
        )
      ],
    );
  }

  Map<String, String> get pipelineOptionsListValue {
    final notEmptyOptions = pipelineOptionsList
        .where((option) =>
            option.name.text.isNotEmpty && option.value.text.isNotEmpty)
        .toList();
    return {for (var item in notEmptyOptions) item.name.text: item.value.text};
  }

  String get pipelineOptionsValue {
    if (selectedTab == kRawTabIndex) {
      return pipelineOptionsController.text;
    }
    return pipelineOptionsToString(pipelineOptionsListValue);
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
      pipelineOptionsList = parsedOptions;
    }
  }

  List<PipelineOptionController> _pipelineOptionsMapToList(
      String pipelineOptions) {
    return parsePipelineOptions(widget.pipelineOptions)
            ?.entries
            .map((e) => PipelineOptionController(name: e.key, value: e.value))
            .toList() ??
        [];
  }
}
