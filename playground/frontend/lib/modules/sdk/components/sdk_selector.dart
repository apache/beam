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
import 'package:playground/modules/sdk/models/sdk.dart';

typedef SetSdk = void Function(SDK sdk);

class SDKSelector extends StatelessWidget {
  final SDK sdk;
  final SetSdk setSdk;

  const SDKSelector({Key? key, required this.sdk, required this.setSdk})
      : super(key: key);

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
      child: DropdownButtonHideUnderline(
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
  }
}
