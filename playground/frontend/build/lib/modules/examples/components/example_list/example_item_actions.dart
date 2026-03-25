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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:flutter_svg/flutter_svg.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import '../../../../src/assets/assets.gen.dart';
import '../../models/popover_state.dart';
import '../description_popover/description_popover_button.dart';
import '../multi_file_icon.dart';

class ExampleItemActions extends StatelessWidget {
  final ExampleBase example;
  final BuildContext parentContext;

  const ExampleItemActions(
      {Key? key, required this.parentContext, required this.example})
      : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Row(
      children: [
        if (example.isMultiFile) const _IconWrapper(MultiFileIcon()),
        if (example.usesEmulatedData) const _IconWrapper(_EmulatedDataIcon()),
        if (example.complexity != null)
          _IconWrapper(ComplexityWidget(complexity: example.complexity!)),
        descriptionPopover,
      ],
    );
  }

  Widget get descriptionPopover => DescriptionPopoverButton(
        parentContext: parentContext,
        example: example,
        followerAnchor: Alignment.topLeft,
        targetAnchor: Alignment.topRight,
        onOpen: () => _setPopoverOpen(parentContext, true),
        onClose: () => _setPopoverOpen(parentContext, false),
      );

  void _setPopoverOpen(BuildContext context, bool isOpen) {
    Provider.of<PopoverState>(context, listen: false).setOpen(isOpen);
  }
}

class _EmulatedDataIcon extends StatelessWidget {
  const _EmulatedDataIcon();

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () {},
      child: Tooltip(
        message: 'intents.playground.usesEmulatedData'.tr(),
        child: SvgPicture.asset(
          Assets.streaming,
          color: Theme.of(context).extension<BeamThemeExtension>()?.iconColor,
        ),
      ),
    );
  }
}

/// A wrapper of a standard size for icons in the example list.
class _IconWrapper extends StatelessWidget {
  const _IconWrapper(this.child);

  final Widget child;

  static const double _iconSize = 30;

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      height: _iconSize,
      width: _iconSize,
      child: Center(
        child: child,
      ),
    );
  }
}
