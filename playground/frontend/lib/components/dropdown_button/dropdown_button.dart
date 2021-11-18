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
import 'package:playground/modules/examples/models/selector_size_model.dart';

const int kAnimationDurationInMilliseconds = 80;
const Offset kAnimationBeginOffset = Offset(0.0, -0.02);
const Offset kAnimationEndOffset = Offset(0.0, 0.0);
const double kAdditionalDyAlignment = 50.0;

class AppDropdownButton extends StatefulWidget {
  final Widget buttonText;
  final Function createDropdown;
  final double height;
  final double width;

  const AppDropdownButton({
    Key? key,
    required this.buttonText,
    required this.createDropdown,
    required this.height,
    required this.width,
  }) : super(key: key);

  @override
  _AppDropdownButtonState createState() => _AppDropdownButtonState();
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
    return Container(
      height: kContainerHeight,
      decoration: BoxDecoration(
        color: ThemeColors.of(context).greyColor,
        borderRadius: BorderRadius.circular(kSmBorderRadius),
      ),
      child: TextButton(
        key: selectorKey,
        onPressed: _changeSelectorVisibility,
        child: Padding(
          padding: const EdgeInsets.all(kMdSpacing),
          child: Wrap(
            alignment: WrapAlignment.center,
            crossAxisAlignment: WrapCrossAlignment.center,
            children: [
              widget.buttonText,
              const Icon(Icons.keyboard_arrow_down),
            ],
          ),
        ),
      ),
    );
  }

  OverlayEntry createDropdown() {
    SelectorPositionModel posModel = findSelectorPositionData();

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
              left: posModel.xAlignment,
              top: posModel.yAlignment + kAdditionalDyAlignment,
              child: SlideTransition(
                position: offsetAnimation,
                child: Material(
                  elevation: kElevation,
                  borderRadius: BorderRadius.circular(kMdBorderRadius),
                  child: Container(
                    height: widget.height,
                    width: widget.width,
                    decoration: BoxDecoration(
                      color: Theme.of(context).backgroundColor,
                      borderRadius: BorderRadius.circular(kMdBorderRadius),
                    ),
                    child: widget.createDropdown(_close),
                  ),
                ),
              ),
            ),
          ],
        );
      },
    );
  }

  SelectorPositionModel findSelectorPositionData() {
    RenderBox? rBox =
        selectorKey.currentContext?.findRenderObject() as RenderBox;
    SelectorPositionModel positionModel = SelectorPositionModel(
      xAlignment: rBox.localToGlobal(Offset.zero).dx,
      yAlignment: rBox.localToGlobal(Offset.zero).dy,
    );
    return positionModel;
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
