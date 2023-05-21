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

import 'dart:async';

import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import '../../constants/sizes.dart';
import '../../pages/standalone_playground/notifiers/example_selector_state.dart';
import 'components/outside_click_handler.dart';
import 'examples_dropdown_content.dart';
import 'models/popover_state.dart';

const double kLgContainerHeight = 490.0;
const double kLgContainerWidth = 400.0;

class ExampleSelector extends StatefulWidget {
  final bool isSelectorOpened;
  final PlaygroundController playgroundController;

  const ExampleSelector({
    required this.isSelectorOpened,
    required this.playgroundController,
  });

  @override
  State<ExampleSelector> createState() => _ExampleSelectorState();
}

class _ExampleSelectorState extends State<ExampleSelector> {
  final _selectorKey = LabeledGlobalKey('ExampleSelector');
  OverlayEntry? _overlayEntry;

  @override
  Widget build(BuildContext context) {
    return Container(
      height: kContainerHeight,
      decoration: BoxDecoration(
        color: Theme.of(context).dividerColor,
        borderRadius: BorderRadius.circular(kSmBorderRadius),
      ),
      child: ChangeNotifierProvider<PlaygroundController>.value(
        value: widget.playgroundController,
        builder: (context, child) => TextButton(
          key: _selectorKey,
          onPressed: () {
            if (widget.isSelectorOpened) {
              _overlayEntry?.remove();
              widget.playgroundController.exampleCache.setSelectorOpened(false);
            } else {
              unawaited(_loadCatalogIfNot(widget.playgroundController));
              _overlayEntry = _createExamplesDropdown();
              Overlay.of(context).insert(_overlayEntry!);
              widget.playgroundController.exampleCache.setSelectorOpened(true);
            }
          },
          child: Wrap(
            alignment: WrapAlignment.center,
            crossAxisAlignment: WrapCrossAlignment.center,
            children: [
              Text(widget.playgroundController.examplesTitle),
              const Icon(Icons.keyboard_arrow_down),
            ],
          ),
        ),
      ),
    );
  }

  Future<void> _loadCatalogIfNot(PlaygroundController controller) async {
    try {
      await controller.exampleCache.loadAllPrecompiledObjectsIfNot();
    } on Exception catch (ex) {
      PlaygroundComponents.toastNotifier.addException(ex);
    }
  }

  OverlayEntry _createExamplesDropdown() {
    Offset dropdownOffset = findDropdownOffset(key: _selectorKey);

    return OverlayEntry(
      builder: (context) {
        return ChangeNotifierProvider<PopoverState>(
          create: (context) => PopoverState(false),
          builder: (context, state) {
            return ChangeNotifierProvider<PlaygroundController>.value(
              value: widget.playgroundController,
              builder: (context, child) => Stack(
                children: [
                  OutsideClickHandler(
                    onTap: () {
                      _closeDropdown(widget.playgroundController.exampleCache);
                      // handle description dialogs
                      Navigator.of(context, rootNavigator: true)
                          .popUntil((route) {
                        return route.isFirst;
                      });
                    },
                  ),
                  ChangeNotifierProvider(
                    create: (context) => ExampleSelectorState(
                      widget.playgroundController,
                      widget.playgroundController.exampleCache
                          .getCategories(widget.playgroundController.sdk),
                    ),
                    builder: (context, _) => Positioned(
                      left: dropdownOffset.dx,
                      top: dropdownOffset.dy,
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
                          child: ExamplesDropdownContent(
                            onSelected: () => _closeDropdown(
                              widget.playgroundController.exampleCache,
                            ),
                            playgroundController: widget.playgroundController,
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

  void _closeDropdown(ExampleCache exampleCache) {
    _overlayEntry?.remove();
    _overlayEntry = null;
    exampleCache.setSelectorOpened(false);
  }
}
