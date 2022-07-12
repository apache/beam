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
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/analytics/google_analytics_service.dart';
import 'package:playground/modules/editor/repository/code_repository/code_client/grpc_code_client.dart';
import 'package:playground/modules/editor/repository/code_repository/code_repository.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/examples/repositories/example_client/grpc_example_client.dart';
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/modules/output/models/output_placement_state.dart';
import 'package:playground/pages/playground/states/examples_state.dart';
import 'package:playground/pages/playground/states/feedback_state.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

final CodeRepository kCodeRepository = CodeRepository(GrpcCodeClient());
final ExampleRepository kExampleRepository =
    ExampleRepository(GrpcExampleClient());

class PlaygroundPageProviders extends StatelessWidget {
  final Widget child;

  const PlaygroundPageProviders({
    Key? key,
    required this.child,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return MultiProvider(
      providers: [
        Provider<AnalyticsService>(create: (context) => GoogleAnalyticsService()),
        ChangeNotifierProvider<ExampleState>(
          create: (context) => ExampleState(kExampleRepository)..init(),
        ),
        ChangeNotifierProxyProvider<ExampleState, PlaygroundState>(
          create: (context) => PlaygroundState(codeRepository: kCodeRepository),
          update: (context, exampleState, playground) {
            if (playground == null) {
              return PlaygroundState(codeRepository: kCodeRepository);
            }

            _onExampleStateChanged(exampleState, playground);
            return playground;
          },
        ),
        ChangeNotifierProvider<OutputPlacementState>(
          create: (context) => OutputPlacementState(),
        ),
        ChangeNotifierProvider<FeedbackState>(
          create: (context) => FeedbackState(),
        ),
      ],
      child: child,
    );
  }

  void _onExampleStateChanged(
    ExampleState exampleState,
    PlaygroundState playgroundState,
  ) {
    // This property currently doubles as a flag of initialization
    // because it is initialized when an example is ready
    // and is filled with a null-object if not showing any example.
    //
    // TODO: Add a dedicated flag of initialization or make
    //       PlaygroundState listen for examples and init itself.
    if (playgroundState.selectedExample != null) {
      return; // Already initialized.
    }

    if (_isEmbedded()) {
      _initEmbedded(exampleState, playgroundState);
    } else {
      _initNonEmbedded(exampleState, playgroundState);
    }
  }

  bool _isEmbedded() {
    return Uri.base.toString().contains(kIsEmbedded);
  }

  Future<void> _initEmbedded(
    ExampleState exampleState,
    PlaygroundState playgroundState,
  ) async {
    final example = _getEmbeddedExample();

    if (example.path.isEmpty) {
      String source = Uri.base.queryParameters[kSourceCode] ?? '';
      example.setSource(source);
      playgroundState.setExample(example);
    } else {
      final loadedExample = await exampleState.getExample(
        example.path,
        playgroundState.sdk,
      );

      final exampleWithInfo = await exampleState.loadExampleInfo(
        loadedExample,
        playgroundState.sdk,
      );

      playgroundState.setExample(exampleWithInfo);
    }
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

  Future<void> _initNonEmbedded(
    ExampleState exampleState,
    PlaygroundState playgroundState,
  ) async {
    await exampleState.loadDefaultExamplesIfNot();

    final example = await _getExample(exampleState, playgroundState);

    if (example == null) {
      return;
    }

    final exampleWithInfo = await exampleState.loadExampleInfo(
      example,
      playgroundState.sdk,
    );

    playgroundState.setExample(exampleWithInfo);
  }

  Future<ExampleModel?> _getExample(
    ExampleState exampleState,
    PlaygroundState playground,
  ) async {
    final examplePath = Uri.base.queryParameters[kExampleParam];

    if (examplePath?.isEmpty ?? true) {
      return exampleState.defaultExamplesMap[playground.sdk];
    }

    final allExamples = exampleState.sdkCategories?.values
        .expand((sdkCategory) => sdkCategory.map((e) => e.examples))
        .expand((element) => element)
        .toList();

    if (allExamples?.isEmpty ?? true) {
      return null;
    }

    return allExamples?.firstWhere(
      (example) => example.path == examplePath,
      orElse: () => exampleState.defaultExample!,
    );
  }
}
