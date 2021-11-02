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
import 'package:playground/config/theme.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:playground/pages/playground/states/examples_state.dart';
import 'package:provider/provider.dart';

typedef SetSdk = void Function(SDK sdk);
typedef SetExample = void Function(ExampleModel example);

class SDKSelector extends StatelessWidget {
  final SDK sdk;
  final SetSdk setSdk;
  final SetExample setExample;

  const SDKSelector({
    Key? key,
    required this.sdk,
    required this.setSdk,
    required this.setExample,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(
        vertical: kZeroSpacing,
        horizontal: kXlSpacing,
      ),
      decoration: BoxDecoration(
        color: ThemeColors.of(context).greyColor,
        borderRadius: BorderRadius.circular(kLgBorderRadius),
      ),
      child: Consumer<ExampleState>(
        builder: (context, state, child) => DropdownButtonHideUnderline(
          child: DropdownButton<SDK>(
            value: sdk,
            icon: const Icon(Icons.keyboard_arrow_down),
            iconSize: kIconSizeMd,
            elevation: kElevation,
            borderRadius: BorderRadius.circular(kLgBorderRadius),
            alignment: Alignment.bottomCenter,
            onChanged: (SDK? newSdk) {
              if (newSdk != null) {
                setSdk(newSdk);
                setExample(state.sdkCategories![newSdk]!.first.examples.first);
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
      ),
    );
  }
}
