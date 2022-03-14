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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:playground/config/theme.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover.dart';
import 'package:playground/modules/examples/models/example_model.dart';

class DescriptionPopoverButton extends StatelessWidget {
  final BuildContext? parentContext;
  final ExampleModel example;
  final Alignment followerAnchor;
  final Alignment targetAnchor;
  final void Function()? onOpen;
  final void Function()? onClose;

  const DescriptionPopoverButton({
    Key? key,
    this.parentContext,
    required this.example,
    required this.followerAnchor,
    required this.targetAnchor,
    this.onOpen,
    this.onClose,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;
    return Semantics(
      container: true,
      child: IconButton(
        iconSize: kIconSizeMd,
        splashRadius: kIconButtonSplashRadius,
        icon: Icon(
          Icons.info_outline_rounded,
          color: ThemeColors.of(context).grey1Color,
        ),
        tooltip: appLocale.exampleDescription,
        onPressed: () {
          _showDescriptionPopover(
            parentContext ?? context,
            example,
            followerAnchor,
            targetAnchor,
          );
        },
      ),
    );
  }

  void _showDescriptionPopover(
    BuildContext context,
    ExampleModel example,
    Alignment followerAnchor,
    Alignment targetAnchor,
  ) async {
    // close previous description dialog
    Navigator.of(context, rootNavigator: true).popUntil((route) {
      return route.isFirst;
    });
    if (onOpen != null) {
      onOpen!();
    }
    await showAlignedDialog(
      context: context,
      builder: (dialogContext) => DescriptionPopover(
        example: example,
      ),
      followerAnchor: followerAnchor,
      targetAnchor: targetAnchor,
      barrierColor: Colors.transparent,
    );
    if (onClose != null) {
      onClose!();
    }
  }
}
