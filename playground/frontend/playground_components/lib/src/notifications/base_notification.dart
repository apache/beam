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

import '../constants/sizes.dart';

const kNotificationBorderWidth = 4.0;
const kMaxTextWidth = 300.0;

class BaseNotification extends StatelessWidget {
  final String title;
  final String notification;
  final Color color;
  final String asset;

  const BaseNotification({
    super.key,
    required this.title,
    required this.notification,
    required this.color,
    required this.asset,
  });

  @override
  Widget build(BuildContext context) {
    return Stack(
      children: [
        _renderLeftBorder(context),
        _renderNotificationContent(context),
      ],
    );
  }

  Widget _renderLeftBorder(BuildContext context) {
    return Positioned(
      width: kNotificationBorderWidth,
      left: 0,
      top: 0,
      bottom: 0,
      child: Container(
        decoration: BoxDecoration(
          color: color,
          borderRadius: const BorderRadius.only(
            topLeft: Radius.circular(BeamSizes.size8),
            bottomLeft: Radius.circular(BeamSizes.size8),
          ),
        ),
      ),
    );
  }

  Widget _renderNotificationContent(BuildContext context) {
    final textTheme = Theme.of(context).textTheme.bodyText1;
    return Positioned(
      child: Padding(
        padding: const EdgeInsets.all(BeamSizes.size12),
        child: Row(
          children: [
            SvgPicture.asset(asset),
            const SizedBox(width: BeamSizes.size12),
            Wrap(
              direction: Axis.vertical,
              spacing: BeamSizes.size4,
              children: [
                Text(
                  title,
                  style: textTheme?.copyWith(fontWeight: FontWeight.w600),
                ),
                SizedBox(
                  width: kMaxTextWidth,
                  child: SelectableText(
                    notification,
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
