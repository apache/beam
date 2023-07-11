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
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/modules/editor/components/share_dropdown/link_text_field.dart';
import 'package:playground/modules/editor/components/share_dropdown/share_button.dart';
import 'package:playground/modules/editor/components/share_dropdown/share_tabs_headers.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover_button.dart';
import 'package:playground/modules/examples/example_selector.dart';
import 'package:playground/modules/sdk/components/sdk_selector.dart';
import 'package:playground/modules/sdk/components/sdk_selector_row.dart';
import 'package:playground/modules/shortcuts/components/shortcuts_dialog.dart';
import 'package:playground/pages/standalone_playground/widgets/editor_textarea_wrapper.dart';
import 'package:playground/pages/standalone_playground/widgets/more_actions.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

extension CommonFindersExtension on CommonFinders {
  Finder codeTextAreaWrapper() {
    return byType(CodeTextAreaWrapper);
  }

  Finder copyButton() {
    return byType(CopyButton);
  }

  Finder descriptionPopoverButton() {
    return byType(DescriptionPopoverButton);
  }

  Finder descriptionPopover() {
    return byType(DescriptionPopover);
  }

  Finder dragHandle() {
    return byType(DragHandle);
  }

  Finder exampleItemInDropdown(String name) {
    return widgetWithText(GestureDetector, name);
  }

  Finder exampleSelector() {
    return byType(ExampleSelector);
  }

  Finder dismissibleOverlay() {
    return find.byKey(BeamOverlay.dismissibleAreaKey);
  }

  Finder feedbackDropdownContent() {
    return byType(FeedbackDropdown);
  }

  Finder feedbackDropdownSendButton() {
    return find.byKey(FeedbackDropdown.sendButtonKey);
  }

  Finder feedbackDropdownTextField() {
    return find.byKey(FeedbackDropdown.textFieldKey);
  }

  Finder feedbackThumb(FeedbackRating rating) {
    return find.byType(InkWell).and(
          find.byKey(Key(rating.name)),
        );
  }

  Finder moreActions() {
    return byType(MoreActions);
  }

  Finder menuItem(HeaderAction value) {
    return byWidgetPredicate(
      (widget) => widget is PopupMenuItem && widget.value == value,
    );
  }

  Finder pipelineOptionsDropdown() {
    return find.byType(PipelineOptionsDropdown);
  }

  Finder pipelineOptionsListTab() {
    return find.byKey(PipelineOptionsDropdownBody.optionsTabKey);
  }

  Finder pipelineOptionsRawTab() {
    return find.byKey(PipelineOptionsDropdownBody.rawTabKey);
  }

  Finder pipelineOptionsSaveAndCloseButton() {
    return find.byKey(PipelineOptionsDropdownBody.saveAndCloseButtonKey);
  }

  Finder sdkItemInDropdown(Sdk sdk) {
    return find.byType(SdkSelectorRow).and(find.byKey(ValueKey(sdk)));
  }

  Finder sdkSelector() {
    return byType(SDKSelector);
  }

  Finder shareableTextField() {
    return byType(LinkTextField);
  }

  Finder shareButton() {
    return byType(ShareButton);
  }

  Finder shareEmbedTabHeader() {
    // TODO(alexeyinkin): Use keys when refactored the sharing tabs, https://github.com/apache/beam/issues/24637
    return descendant(
      of: byType(ShareTabsHeaders),
      matching: text('Embed'),
    );
  }

  Finder shortcutsModal() {
    return byType(ShortcutsDialogContent);
  }
}
