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
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/example_selector.dart';
import 'package:playground/pages/playground/components/feedback/feedback_dropdown_content.dart';

const double kFeedbackTitleFontSize = 24.0;
const double kFeedbackContentFontSize = 14.0;
const double kFeedbackDyBottomAlignment = 50.0;
const double kFeedbackDxLeftAlignment = 10.0;
const double kFeedbackDropdownWidth = 400.0;

class FeedbackDropdownIconButton extends StatefulWidget {
  final String label;
  final String iconAsset;
  final String filledIconAsset;
  final bool isSelected;
  final void Function() onClick;

  const FeedbackDropdownIconButton({
    Key? key,
    required this.label,
    required this.iconAsset,
    required this.filledIconAsset,
    required this.isSelected,
    required this.onClick,
  }) : super(key: key);

  @override
  State<FeedbackDropdownIconButton> createState() =>
      _FeedbackDropdownIconButton();
}

class _FeedbackDropdownIconButton extends State<FeedbackDropdownIconButton>
    with TickerProviderStateMixin {
  final GlobalKey feedbackKey = LabeledGlobalKey('FeedbackDropdown');
  final TextEditingController feedbackTextController = TextEditingController();
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
    return Semantics(
      container: true,
      child: IconButton(
        key: feedbackKey,
        padding: EdgeInsets.zero,
        onPressed: () {
          _changeSelectorVisibility();
          widget.onClick();
        },
        tooltip: widget.label,
        icon: SvgPicture.asset(
          widget.isSelected ? widget.filledIconAsset : widget.iconAsset,
        ),
      ),
    );
  }

  OverlayEntry createDropdown() {
    return OverlayEntry(
      builder: (context) {
        return Stack(
          children: [
            GestureDetector(
              onTap: () {
                _close();
              },
              child: Container(
                color: Colors.transparent,
                height: double.infinity,
                width: double.infinity,
              ),
            ),
            Positioned(
              left: kFeedbackDxLeftAlignment,
              bottom: kFeedbackDyBottomAlignment,
              child: SlideTransition(
                position: offsetAnimation,
                child: Material(
                  elevation: kElevation * 2,
                  borderRadius: BorderRadius.circular(kMdBorderRadius),
                  child: Container(
                    width: kFeedbackDropdownWidth,
                    decoration: BoxDecoration(
                      color: Theme.of(context).backgroundColor,
                      borderRadius: BorderRadius.circular(kMdBorderRadius),
                    ),
                    child: FeedbackDropdownContent(
                      close: _close,
                      textController: feedbackTextController,
                    ),
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
    feedbackTextController.clear();
  }

  void _open() {
    animationController.forward();
    dropdown = createDropdown();
    Overlay.of(context)?.insert(dropdown!);
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
