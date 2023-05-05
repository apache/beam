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

import '../../constants/sizes.dart';
import '../unread/marker.dart';

const _horizontalPadding = BeamSizes.size8;

class OutputTab extends StatelessWidget {
  const OutputTab({
    required this.isUnread,
    required this.title,
    this.trailing,
  });

  final bool isUnread;
  final String title;
  final Widget? trailing;

  @override
  Widget build(BuildContext context) {
    return Tab(
      child: Wrap(
        alignment: WrapAlignment.center,
        spacing: BeamSizes.size8,
        children: [
          const SizedBox(width: _horizontalPadding),
          Text(title),
          if (trailing != null) trailing!,
          SizedBox(
            width: _horizontalPadding,
            child: AnimatedSwitcher(
              duration: const Duration(milliseconds: 500),
              child: isUnread
                  ? const UnreadMarkerWidget()
                  : const Opacity(opacity: 0, child: UnreadMarkerWidget()),
            ),
          ),
        ],
      ),
    );
  }
}
