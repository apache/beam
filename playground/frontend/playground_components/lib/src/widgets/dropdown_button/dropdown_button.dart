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
import '../../theme/theme.dart';
import '../../util/dropdown_utils.dart';

const int kAnimationDurationInMilliseconds = 80;
const Offset kAnimationBeginOffset = Offset(0, -0.02);
const Offset kAnimationEndOffset = Offset.zero;

/// How to align the button and its dropdown.
enum DropdownAlignment {
  /// Align the left edges of the button and its dropdown.
  left,

  /// Align the right edges of the button and its dropdown.
  right,
}

class AppDropdownButton extends StatefulWidget {
  final Widget buttonText;
  final EdgeInsets buttonPadding;
  final Widget Function(void Function() close) createDropdown;
  final DropdownAlignment dropdownAlign;
  final double? height;
  final Widget? leading;
  final bool showArrow;
  final double width;

  const AppDropdownButton({
    super.key,
    required this.buttonText,
    required this.createDropdown,
    required this.width,
    this.buttonPadding = const EdgeInsets.all(BeamSpacing.medium),
    this.dropdownAlign = DropdownAlignment.left,
    this.height,
    this.leading,
    this.showArrow = true,
  });

  @override
  State<AppDropdownButton> createState() => _AppDropdownButtonState();
}

class _AppDropdownButtonState extends State<AppDropdownButton>
    with TickerProviderStateMixin {
  final GlobalKey selectorKey = LabeledGlobalKey('ExampleSelector');
  late OverlayEntry? dropdown;
  late AnimationController animationController;
  late Animation<Offset> offsetAnimation;
  bool isOpen = false;

  @override
  void initState() {
    super.initState();
    animationController = AnimationController(
      vsync: this,
      duration: const Duration(milliseconds: kAnimationDurationInMilliseconds),
    );
    offsetAnimation = Tween<Offset>(
      begin: kAnimationBeginOffset,
      end: kAnimationEndOffset,
    ).animate(animationController);
  }

  @override
  void dispose() {
    animationController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final ext = Theme.of(context).extension<BeamThemeExtension>()!;

    return Container(
      height: BeamSizes.buttonHeight,
      decoration: BoxDecoration(
        color: ext.fieldBackgroundColor,
        borderRadius: BorderRadius.circular(BeamBorderRadius.small),
      ),
      child: TextButton(
        key: selectorKey,
        onPressed: _changeSelectorVisibility,
        child: Padding(
          padding: widget.buttonPadding,
          child: Wrap(
            alignment: WrapAlignment.center,
            crossAxisAlignment: WrapCrossAlignment.center,
            children: [
              if (widget.leading != null)
                Padding(
                  padding: const EdgeInsets.only(right: BeamSpacing.medium),
                  child: widget.leading,
                ),
              widget.buttonText,
              if (widget.showArrow) const Icon(Icons.keyboard_arrow_down),
            ],
          ),
        ),
      ),
    );
  }

  OverlayEntry createDropdown() {
    final dropdownOffset = findDropdownOffset(
      alignment: widget.dropdownAlign,
      key: selectorKey,
      widgetWidth: widget.width,
    );

    final child = widget.createDropdown(_close);

    return OverlayEntry(
      builder: (context) {
        return Stack(
          children: [
            GestureDetector(
              onTap: _close,
              child: Container(
                color: Colors.transparent,
                height: double.infinity,
                width: double.infinity,
              ),
            ),
            Positioned(
              left: dropdownOffset.dx,
              top: dropdownOffset.dy,
              child: SlideTransition(
                position: offsetAnimation,
                child: Material(
                  elevation: BeamSizes.elevation,
                  borderRadius: BorderRadius.circular(BeamBorderRadius.medium),
                  child: Container(
                    height: widget.height,
                    width: widget.width,
                    decoration: BoxDecoration(
                      color: Theme.of(context).backgroundColor,
                      borderRadius: BorderRadius.circular(
                        BeamBorderRadius.medium,
                      ),
                    ),
                    child: child,
                  ),
                ),
              ),
            ),
          ],
        );
      },
    );
  }

  void _close() {
    animationController.reverse();
    dropdown?.remove();
    setState(() {
      isOpen = false;
    });
  }

  void _open() {
    animationController.forward();
    dropdown = createDropdown();
    Overlay.of(context).insert(dropdown!);
    setState(() {
      isOpen = true;
    });
  }

  void _changeSelectorVisibility() {
    if (isOpen) {
      _close();
    } else {
      _open();
    }
  }
}
