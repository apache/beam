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
import 'package:playground/config.g.dart';
import 'package:playground/constants/params.dart';
import 'package:playground/modules/analytics/analytics_service.dart';
import 'package:playground/modules/analytics/google_analytics_service.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground/modules/messages/handlers/messages_debouncer.dart';
import 'package:playground/modules/messages/handlers/messages_handler.dart';
import 'package:playground/modules/messages/listeners/messages_listener.dart';
import 'package:playground/modules/output/models/output_placement_state.dart';
import 'package:playground/pages/playground/states/feedback_state.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

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
        Provider<AnalyticsService>(
          create: (context) => GoogleAnalyticsService(),
        ),
        ChangeNotifierProvider<PlaygroundController>(
          create: (context) {
            final codeRepository = CodeRepository(
              client: GrpcCodeClient(
                url: kApiClientURL,
                runnerUrlsById: {
                  Sdk.java.id: kApiJavaClientURL,
                  Sdk.go.id: kApiGoClientURL,
                  Sdk.python.id: kApiPythonClientURL,
                  Sdk.scio.id: kApiScioClientURL,
                },
              ),
            );

            final exampleRepository = ExampleRepository(
              client: GrpcExampleClient(url: kApiClientURL),
            );

            final exampleCache = ExampleCache(
              exampleRepository: exampleRepository,
              hasCatalog: !isEmbedded(),
            )..init();

            final controller = PlaygroundController(
              examplesLoader: ExamplesLoader(),
              exampleCache: exampleCache,
              codeRepository: codeRepository,
            );

            final descriptor = ExamplesLoadingDescriptorFactory.fromUriParts(
              path: Uri.base.path,
              params: Uri.base.queryParameters,
            );
            controller.examplesLoader.load(descriptor);

            final handler = MessagesDebouncer(
              handler: MessagesHandler(playgroundController: controller),
            );
            MessagesListener(handler: handler);

            return controller;
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
}
