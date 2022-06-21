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
import 'package:playground/constants/params.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/output/models/output_placement_state.dart';
import 'package:playground/pages/embedded_playground/embedded_playground_page.dart';
import 'package:playground/pages/playground/components/playground_page_providers.dart';
import 'package:playground/pages/playground/states/examples_state.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

class EmbeddedPageProviders extends StatelessWidget {
  const EmbeddedPageProviders({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return MultiProvider(
      providers: [
        ChangeNotifierProvider<ExampleState>(
          create: (context) => ExampleState(kExampleRepository),
        ),
        ChangeNotifierProxyProvider<ExampleState, PlaygroundState>(
          create: (context) => PlaygroundState(codeRepository: kCodeRepository),
          update: (context, exampleState, playground) {
            if (playground == null) {
              return PlaygroundState(codeRepository: kCodeRepository);
            }

            if (playground.selectedExample == null) {
              final example = _getEmbeddedExample();
              _loadExampleData(
                example,
                exampleState,
                playground,
              );
            }
            return playground;
          },
        ),
        ChangeNotifierProvider<OutputPlacementState>(
          create: (context) => OutputPlacementState(),
        ),
      ],
      child: EmbeddedPlaygroundPage(
        isEditable: _isEditableToBool(),
      ),
    );
  }

  bool _isEditableToBool() {
    final isEditableString = Uri.base.queryParameters[kIsEditable];
    return isEditableString == 'true';
  }

  ExampleModel _getEmbeddedExample() {
    final examplePath = Uri.base.queryParameters[kExampleParam];

    return ExampleModel(
      name: 'Embedded_Example',
      path: examplePath ?? '',
      description: '',
      type: ExampleType.example,
    );
  }

  _loadExampleData(
    ExampleModel? example,
    ExampleState exampleState,
    PlaygroundState playground,
  ) {
    if (example == null) {
      return;
    }

    if (example.path.isEmpty) {
      String source = Uri.base.queryParameters[kSourceCode] ?? '';
      example.setSource(source);
      playground.setExample(example);
    } else {
      exampleState
          .getExample(example.path, playground.sdk)
          .then((example) => exampleState.loadExampleInfo(
                example,
                playground.sdk,
              ))
          .then((exampleWithInfo) => playground.setExample(exampleWithInfo));
    }
  }
}
