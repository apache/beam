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
import 'package:playground/components/loading_indicator/loading_indicator.dart';
import 'package:playground/config/theme.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/components/examples_components.dart';
import 'package:playground/modules/examples/models/selector_size_model.dart';
import 'package:playground/pages/playground/states/example_selector_state.dart';
import 'package:playground/pages/playground/states/examples_state.dart';
import 'package:playground/pages/playground/states/playground_state.dart';
import 'package:provider/provider.dart';

const int kAnimationDurationInMilliseconds = 80;
const Offset kAnimationBeginOffset = Offset(0.0, -0.02);
const Offset kAnimationEndOffset = Offset(0.0, 0.0);
const double kAdditionalDyAlignment = 50.0;
const double kLgContainerHeight = 444.0;
const double kLgContainerWidth = 400.0;

class ExampleSelector extends StatefulWidget {
  final Function changeSelectorVisibility;
  final bool isSelectorOpened;

  const ExampleSelector({
    Key? key,
    required this.changeSelectorVisibility,
    required this.isSelectorOpened,
  }) : super(key: key);

  @override
  State<ExampleSelector> createState() => _ExampleSelectorState();
}

class _ExampleSelectorState extends State<ExampleSelector>
    with TickerProviderStateMixin {
  final GlobalKey selectorKey = LabeledGlobalKey('ExampleSelector');
  late OverlayEntry? examplesDropdown;
  late AnimationController animationController;
  late Animation<Offset> offsetAnimation;

  final TextEditingController textController = TextEditingController();
  final ScrollController scrollController = ScrollController();

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
    textController.dispose();
    scrollController.dispose();
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
      child: Consumer<PlaygroundState>(
        builder: (context, state, child) => TextButton(
          key: selectorKey,
          onPressed: () {
            if (widget.isSelectorOpened) {
              animationController.reverse();
              examplesDropdown?.remove();
            } else {
              animationController.forward();
              examplesDropdown = createExamplesDropdown();
              Overlay.of(context)?.insert(examplesDropdown!);
            }
            widget.changeSelectorVisibility();
          },
          child: Wrap(
            alignment: WrapAlignment.center,
            crossAxisAlignment: WrapCrossAlignment.center,
            children: [
              Consumer<PlaygroundState>(
                builder: (context, state, child) => Text(state.examplesTitle),
              ),
              const Icon(Icons.keyboard_arrow_down),
            ],
          ),
        ),
      ),
    );
  }

  OverlayEntry createExamplesDropdown() {
    SelectorPositionModel posModel = findSelectorPositionData();

    return OverlayEntry(
      builder: (context) {
        return Consumer2<ExampleState, PlaygroundState>(
          builder: (context, exampleState, playgroundState, child) => Stack(
            children: [
              GestureDetector(
                onTap: () => closeDropdown(exampleState),
                child: Container(
                  color: Colors.transparent,
                  height: double.infinity,
                  width: double.infinity,
                ),
              ),
              ChangeNotifierProvider(
                create: (context) => ExampleSelectorState(
                  exampleState,
                  playgroundState,
                  exampleState.getCategories(playgroundState.sdk)!,
                ),
                builder: (context, _) => Positioned(
                  left: posModel.xAlignment,
                  top: posModel.yAlignment + kAdditionalDyAlignment,
                  child: SlideTransition(
                    position: offsetAnimation,
                    child: Material(
                      elevation: kElevation.toDouble(),
                      child: Container(
                        height: kLgContainerHeight,
                        width: kLgContainerWidth,
                        decoration: BoxDecoration(
                          color: Theme.of(context).backgroundColor,
                          borderRadius: BorderRadius.circular(kMdBorderRadius),
                        ),
                        child: exampleState.sdkCategories == null ||
                                playgroundState.selectedExample == null
                            ? const LoadingIndicator(size: kContainerHeight)
                            : _buildDropdownContent(playgroundState),
                      ),
                    ),
                  ),
                ),
              ),
            ],
          ),
        );
      },
    );
  }

  Widget _buildDropdownContent(PlaygroundState playgroundState) {
    return Column(
      children: [
        SearchField(controller: textController),
        const TypeFilter(),
        ExampleList(
          controller: scrollController,
          selectedExample: playgroundState.selectedExample!,
          animationController: animationController,
          dropdown: examplesDropdown,
        ),
      ],
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

  void closeDropdown(ExampleState exampleState) {
    animationController.reverse();
    examplesDropdown?.remove();
    exampleState.changeSelectorVisibility();
  }
}
