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
import 'package:playground/constants/assets.dart';
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/fonts.dart';
import 'package:playground/constants/sizes.dart';

const double kTitleFontSize = 18;

class Logo extends StatelessWidget {
  const Logo({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    final theme = Theme.of(context);
    return Row(
      mainAxisSize: MainAxisSize.min,
      children: [
        const Image(
          image: AssetImage(kBeamLgIconAsset),
          width: kIconSizeLg,
          height: kIconSizeLg,
        ),
        RichText(
          text: TextSpan(
            style: getTitleFontStyle(
              textStyle: const TextStyle(
                fontSize: kTitleFontSize,
                fontWeight: kLightWeight,
              ),
            ),
            children: <TextSpan>[
              TextSpan(
                text: 'Beam',
                style: TextStyle(color: theme.primaryColor),
              ),
              TextSpan(
                text: ' Playground',
                style: TextStyle(
                  color: theme.textTheme.bodyText1?.color,
                ),
              ),
            ],
          ),
        ),
      ],
    );
  }
}
