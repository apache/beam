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
import 'package:playground/modules/examples/components/description_popover/description_popover_button.dart';
import 'package:playground/modules/examples/components/multifile_popover/multifile_popover_button.dart';
import 'package:playground/modules/examples/models/popover_state.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

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
        if (example.isMultiFile) multifilePopover,
        descriptionPopover,
      ],
    );
  }

  Widget get multifilePopover => MultifilePopoverButton(
        parentContext: parentContext,
        example: example,
        followerAnchor: Alignment.topLeft,
        targetAnchor: Alignment.topRight,
        onOpen: () => _setPopoverOpen(parentContext, true),
        onClose: () => _setPopoverOpen(parentContext, false),
      );

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
