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
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import '../../../modules/output/models/output_placement_state.dart';
import '../notifiers/feedback_state.dart';

/// Sticks commonly needed objects in the widget tree.
class PlaygroundPageProviders extends StatelessWidget {
  final Widget child;
  final PlaygroundController playgroundController;

  const PlaygroundPageProviders({
    required this.child,
    required this.playgroundController,
  });

  @override
  Widget build(BuildContext context) {
    return MultiProvider(
      providers: [
        ChangeNotifierProvider<PlaygroundController>.value(
          value: playgroundController,
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
