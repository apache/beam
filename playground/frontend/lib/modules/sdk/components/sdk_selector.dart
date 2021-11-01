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
import 'package:playground/components/dropdown_button/dropdown_button.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/sdk/components/sdk_selector_row.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

typedef SetSdk = void Function(SDK sdk);

const double kWidth = 150;
const double kHeight = 172;

class SDKSelector extends StatelessWidget {
  final SDK sdk;
  final SetSdk setSdk;

  const SDKSelector({Key? key, required this.sdk, required this.setSdk})
      : super(key: key);

  @override
  Widget build(BuildContext context) {
    return AppDropdownButton(
      buttonText: Text(
        'SDK: ${sdk.displayName}',
      ),
      createDropdown: (close) => Column(
        children: [
          const SizedBox(height: kMdSpacing),
          ...SDK.values.map((SDK value) {
            return SizedBox(
              width: double.infinity,
              child: SdkSelectorRow(
                sdk: value,
                onSelect: () {
                  close();
                  setSdk(value);
                },
              ),
            );
          }).toList()
        ],
      ),
      width: kWidth,
      height: kHeight,
    );
  }
}
