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
import 'package:playground/constants/sizes.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

const kLogText = "Log";
const kGraphText = "Graph";

class OutputArea extends StatelessWidget {
  const OutputArea({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      color: Theme.of(context).backgroundColor,
      child: Consumer<PlaygroundState>(
        builder: (context, state, child) {
          return TabBarView(
            children: <Widget>[
              Padding(
                padding: const EdgeInsets.all(kLgSpacing),
                child: Text(state.result?.output ?? ""),
              ),
              const Center(child: Text(kLogText)),
              const Center(child: Text(kGraphText)),
            ],
          );
        },
      ),
    );
  }
}
