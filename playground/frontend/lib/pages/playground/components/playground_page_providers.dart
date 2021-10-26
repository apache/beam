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
import 'package:playground/modules/editor/repository/code_repository/code_client/grpc_code_client.dart';
import 'package:playground/modules/editor/repository/code_repository/code_repository.dart';
import 'package:playground/modules/output/models/output_placement_state.dart';
import 'package:provider/provider.dart';
import 'package:playground/modules/examples/repositories/example_repository.dart';
import 'package:playground/pages/playground/states/examples_state.dart';
import 'package:playground/pages/playground/states/playground_state.dart';

final CodeRepository kCodeRepository = CodeRepository(GrpcCodeClient());

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
        ChangeNotifierProvider<ExampleState>(
          create: (context) => ExampleState(ExampleRepository()),
        ),
        ChangeNotifierProxyProvider<ExampleState, PlaygroundState>(
          create: (context) => PlaygroundState(codeRepository: kCodeRepository),
          update: (context, exampleState, playground) {
            if (playground == null) {
              return PlaygroundState(codeRepository: kCodeRepository);
            }

            final categories = exampleState.categoriesBySdkMap?[playground.sdk];
            if ((categories?.isNotEmpty ?? false) &&
                playground.selectedExample == null) {

              return PlaygroundState(
                codeRepository: kCodeRepository,
                sdk: playground.sdk,
                selectedExample: categories?.first.examples.first,
              );
            }
            return playground;
          },
        ),
        ChangeNotifierProvider<OutputPlacementState>(
          create: (context) => OutputPlacementState(),
        ),
      ],
      child: child,
    );
  }
}
