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

import 'package:aligned_dialog/aligned_dialog.dart';
import 'package:flutter/material.dart';

import '../constants/colors.dart';
import '../generated/assets.gen.dart';
import 'base_notification.dart';

const kDialogOffset = Offset(0, 30);

class NotificationManager {
  static void showError(
    BuildContext context,
    String title,
    String notification,
  ) {
    return _showNotification(
      context,
      BaseNotification(
        title: title,
        notification: notification,
        color: BeamNotificationColors.error,
        asset: Assets.notificationIcons.error,
      ),
    );
  }

  static void showInfo(
    BuildContext context,
    String title,
    String notification,
  ) {
    return _showNotification(
      context,
      BaseNotification(
        title: title,
        notification: notification,
        color: BeamNotificationColors.info,
        asset: Assets.notificationIcons.info,
      ),
    );
  }

  static void showWarning(
    BuildContext context,
    String title,
    String notification,
  ) {
    return _showNotification(
      context,
      BaseNotification(
        title: title,
        notification: notification,
        color: BeamNotificationColors.warning,
        asset: Assets.notificationIcons.warning,
      ),
    );
  }

  static void showSuccess(
    BuildContext context,
    String title,
    String notification,
  ) {
    return _showNotification(
      context,
      BaseNotification(
        title: title,
        notification: notification,
        color: BeamNotificationColors.success,
        asset: Assets.notificationIcons.success,
      ),
    );
  }

  static void _showNotification(
    BuildContext context,
    Widget content,
  ) {
    showAlignedDialog<void>(
      context: context,
      barrierDismissible: true,
      targetAnchor: Alignment.topCenter,
      offset: kDialogOffset,
      barrierColor: Colors.transparent,
      builder: (BuildContext dialogContext) {
        return AlertDialog(
          contentPadding: EdgeInsets.zero,
          content: content,
        );
      },
    );
  }
}
