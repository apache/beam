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

            if (playground.selectedExample == null &&
                !Uri.base.toString().contains(kIsEmbedded)) {
              final example = _getExample(exampleState, playground);
              if (example != null) {
                exampleState
                    .loadExampleInfo(
                      example,
                      playground.sdk,
                    )
                    .then((exampleWithInfo) =>
                        playground.setExample(exampleWithInfo));
              }
            }
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

  ExampleModel? _getExample(
    ExampleState exampleState,
    PlaygroundState playground,
  ) {
    final examplePath = Uri.base.queryParameters[kExampleParam];

    if (exampleState.defaultExamplesMap.isEmpty) {
      exampleState.loadDefaultExamples();
    }

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
