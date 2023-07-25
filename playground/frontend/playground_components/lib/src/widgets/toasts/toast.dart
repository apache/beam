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
import 'package:flutter_svg/flutter_svg.dart';

import '../../assets/assets.gen.dart';
import '../../constants/colors.dart';
import '../../constants/sizes.dart';
import '../../models/toast.dart';
import '../../models/toast_type.dart';
import '../../playground_components.dart';

const _borderWidth = 4.0;
const _textWidth = 300.0;

const _colors = UnmodifiableToastTypeMap(
  error: BeamNotificationColors.error,
  info: BeamNotificationColors.info,
);

final _iconNames = UnmodifiableToastTypeMap(
  error: Assets.notificationIcons.error,
  info: Assets.notificationIcons.info,
);

/// The content of a popup notification.
///
/// Named after 'fluttertoast' package.
class ToastWidget extends StatelessWidget {
  final Toast toast;

  const ToastWidget(this.toast);

  @override
  Widget build(BuildContext context) {
    return Card(
      child: Stack(
        children: [
          _leftBorder(context),
          _content(context),
        ],
      ),
    );
  }

  Widget _leftBorder(BuildContext context) {
    return Positioned(
      width: _borderWidth,
      left: 0,
      top: 0,
      bottom: 0,
      child: DecoratedBox(
        decoration: BoxDecoration(
          color: _colors.get(toast.type),
          borderRadius: const BorderRadius.only(
            topLeft: Radius.circular(BeamSizes.size8),
            bottomLeft: Radius.circular(BeamSizes.size8),
          ),
        ),
      ),
    );
  }

  Widget _content(BuildContext context) {
    final textTheme = Theme.of(context).textTheme.bodyText1;

    return Positioned(
      child: Padding(
        padding: const EdgeInsets.all(BeamSizes.size12),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            SvgPicture.asset(
              _iconNames.get(toast.type),
              package: PlaygroundComponents.packageName,
            ),
            const SizedBox(width: BeamSizes.size12),
            Wrap(
              direction: Axis.vertical,
              spacing: BeamSizes.size4,
              children: [
                Text(
                  toast.title,
                  style: textTheme?.copyWith(fontWeight: FontWeight.w600),
                ),
                SizedBox(
                  width: _textWidth,
                  child: SelectableText(
                    toast.description,
                    style: textTheme,
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }
}
