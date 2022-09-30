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
import 'package:flutter/widgets.dart';
import 'package:playground_components/playground_components.dart';

// This is for demo only. Need a thought-through import in production.

const String kApiClientURL =
    'https://backend-router-beta-dot-apache-beam-testing.appspot.com';
const String kApiJavaClientURL =
    'https://backend-java-beta-dot-apache-beam-testing.appspot.com';
const String kApiGoClientURL =
    'https://backend-go-beta-dot-apache-beam-testing.appspot.com';
const String kApiPythonClientURL =
    'https://backend-python-beta-dot-apache-beam-testing.appspot.com';
const String kApiScioClientURL =
    'https://backend-scio-beta-dot-apache-beam-testing.appspot.com';

class PlaygroundDemoWidget extends StatefulWidget {
  const PlaygroundDemoWidget({super.key});

  @override
  State<PlaygroundDemoWidget> createState() => _PlaygroundDemoWidgetState();
}

class _PlaygroundDemoWidgetState extends State<PlaygroundDemoWidget> {
  late final PlaygroundController playgroundController;

  @override
  void initState() {
    super.initState();

    final exampleRepository = ExampleRepository(
      client: GrpcExampleClient(url: kApiClientURL),
    );

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

    final exampleCache = ExampleCache(
      exampleRepository: exampleRepository,
      hasCatalog: true,
    );

    playgroundController = PlaygroundController(
      codeRepository: codeRepository,
      exampleCache: exampleCache,
      examplesLoader: ExamplesLoader(),
    );

    playgroundController.examplesLoader.load(
      const ExamplesLoadingDescriptor(
        descriptors: [
          CatalogDefaultExampleLoadingDescriptor(sdk: Sdk.java),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return AnimatedBuilder(
      animation: playgroundController,
      builder: _buildOnChange,
    );
  }

  Widget _buildOnChange(BuildContext context, Widget? child) {
    final snippetController = playgroundController.snippetEditingController;
    if (snippetController == null) {
      return const LoadingIndicator();
    }

    return Stack(
      children: [
        SplitView(
          direction: Axis.vertical,
          first: SnippetEditor(
            controller: snippetController,
            isEditable: true,
            goToContextLine: false,
          ),
          second: OutputWidget(
            playgroundController: playgroundController,
            graphDirection: Axis.horizontal,
          ),
        ),
        Positioned(
          top: 30,
          right: 30,
          child: Row(
            children: [
              RunOrCancelButton(playgroundController: playgroundController),
            ],
          ),
        ),
      ],
    );
  }
}
