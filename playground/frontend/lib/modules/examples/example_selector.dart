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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:playground/constants/links.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/examples/components/examples_components.dart';
import 'package:playground/modules/examples/components/outside_click_handler.dart';
import 'package:playground/modules/examples/models/popover_state.dart';
import 'package:playground/pages/playground/states/example_selector_state.dart';
import 'package:playground/utils/dropdown_utils.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';
import 'package:url_launcher/url_launcher.dart';

const int kAnimationDurationInMilliseconds = 80;
const Offset kAnimationBeginOffset = Offset(0.0, -0.02);
const Offset kAnimationEndOffset = Offset(0.0, 0.0);
const double kAdditionalDyAlignment = 50.0;
const double kLgContainerHeight = 490.0;
const double kLgContainerWidth = 400.0;

class ExampleSelector extends StatefulWidget {
  final void Function() changeSelectorVisibility;
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
        color: Theme.of(context).dividerColor,
        borderRadius: BorderRadius.circular(kSmBorderRadius),
      ),
      child: Consumer<PlaygroundController>(
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
              Consumer<PlaygroundController>(
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
    Offset dropdownOffset = findDropdownOffset(key: selectorKey);

    return OverlayEntry(
      builder: (context) {
        return ChangeNotifierProvider<PopoverState>(
          create: (context) => PopoverState(false),
          builder: (context, state) {
            return Consumer<PlaygroundController>(
              builder: (context, playgroundController, child) => Stack(
                children: [
                  OutsideClickHandler(
                    onTap: () {
                      _closeDropdown(playgroundController.exampleCache);
                      // handle description dialogs
                      Navigator.of(context, rootNavigator: true)
                          .popUntil((route) {
                        return route.isFirst;
                      });
                    },
                  ),
                  ChangeNotifierProvider(
                    create: (context) => ExampleSelectorState(
                      playgroundController,
                      playgroundController.exampleCache
                          .getCategories(playgroundController.sdk),
                    ),
                    builder: (context, _) => Positioned(
                      left: dropdownOffset.dx,
                      top: dropdownOffset.dy,
                      child: SlideTransition(
                        position: offsetAnimation,
                        child: Material(
                          elevation: kElevation,
                          child: Container(
                            height: kLgContainerHeight,
                            width: kLgContainerWidth,
                            decoration: BoxDecoration(
                              color: Theme.of(context).backgroundColor,
                              borderRadius:
                                  BorderRadius.circular(kMdBorderRadius),
                            ),
                            child: _buildDropdownContent(
                              context,
                              playgroundController,
                            ),
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
      },
    );
  }

  Widget _buildDropdownContent(
    BuildContext context,
    PlaygroundController playgroundController,
  ) {
    if (playgroundController.exampleCache.categoryListsBySdk.isEmpty ||
        playgroundController.selectedExample == null) {
      return const LoadingIndicator();
    }

    return Column(
      children: [
        SearchField(controller: textController),
        const TypeFilter(),
        ExampleList(
          controller: scrollController,
          selectedExample: playgroundController.selectedExample!,
          animationController: animationController,
          dropdown: examplesDropdown,
        ),
        const BeamDivider(),
        SizedBox(
          width: double.infinity,
          child: TextButton(
            child: Padding(
              padding: const EdgeInsets.all(kXlSpacing),
              child: Align(
                alignment: Alignment.centerLeft,
                child: Text(
                  AppLocalizations.of(context)!.addExample,
                  style: TextStyle(color: Theme.of(context).primaryColor),
                ),
              ),
            ),
            onPressed: () => launchUrl(Uri.parse(kAddExampleLink)),
          ),
        ),
      ],
    );
  }

  void _closeDropdown(ExampleCache exampleCache) {
    animationController.reverse();
    examplesDropdown?.remove();
    exampleCache.changeSelectorVisibility();
  }
}
