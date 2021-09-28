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
import 'package:provider/provider.dart';
import 'package:playground/constants/sdk.dart';
import 'package:playground/modules/editor/state/editor_state.dart';

class SDKSelector extends StatelessWidget {
  const SDKSelector({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<EditorState>(
      builder: (context, state, child) {
        return Container(
          padding: const EdgeInsets.symmetric(
            vertical: 0.0,
            horizontal: 16.0,
          ),
          decoration: BoxDecoration(
            color: const Color(0x0A091E42),
            borderRadius: BorderRadius.circular(32.0),
          ),
          child: DropdownButtonHideUnderline(
            child: DropdownButton<SDK>(
              value: state.sdk,
              icon: const Icon(Icons.keyboard_arrow_down),
              iconSize: 24,
              elevation: 1,
              borderRadius: BorderRadius.circular(8.0),
              alignment: Alignment.bottomCenter,
              onChanged: (SDK? newSdk) {
                if (newSdk != null) {
                  state.setSdk(newSdk);
                }
              },
              items: SDK.values.map<DropdownMenuItem<SDK>>((SDK value) {
                return DropdownMenuItem<SDK>(
                  value: value,
                  child: Text(value.displayName),
                );
              }).toList(),
            ),
          ),
        );
      },
    );
  }
}
