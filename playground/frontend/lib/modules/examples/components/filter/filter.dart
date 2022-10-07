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

import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:flutter/material.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/components/examples_components.dart';
import 'package:playground/pages/playground/states/example_selector_state.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

class ExamplesFilter extends StatelessWidget {
  const ExamplesFilter({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(
        horizontal: kLgSpacing,
        vertical: kMdSpacing,
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: const [
          _Types(),
          _Tags(),
        ],
      ),
    );
  }
}

class _Types extends StatelessWidget {
  const _Types({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;

    return Row(
      children: [
        TypeBubble(
          type: ExampleType.all,
          name: appLocale.all,
        ),
        TypeBubble(
          type: ExampleType.example,
          name: appLocale.examples,
        ),
        TypeBubble(
          type: ExampleType.kata,
          name: appLocale.katas,
        ),
        TypeBubble(
          type: ExampleType.test,
          name: appLocale.unitTests,
        ),
      ],
    );
  }
}

class _Tags extends StatelessWidget {
  const _Tags({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<ExampleSelectorState>(
      builder: (context, state, child) => Padding(
        padding: const EdgeInsets.symmetric(vertical: kMdSpacing),
        child: SingleChildScrollView(
          scrollDirection: Axis.horizontal,
          child: Row(
            children: state.tags
                .map((tag) => TagBubble(name: tag))
                .toList(growable: false),
          ),
        ),
      ),
    );
  }
}
