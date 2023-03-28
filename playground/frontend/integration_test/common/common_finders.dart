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

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/modules/actions/components/reset_action.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown.dart';
import 'package:playground/modules/editor/components/pipeline_options_dropdown/pipeline_options_dropdown_body.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover_button.dart';
import 'package:playground/modules/examples/example_selector.dart';
import 'package:playground/modules/sdk/components/sdk_selector.dart';
import 'package:playground/modules/sdk/components/sdk_selector_row.dart';
import 'package:playground/modules/shortcuts/components/shortcuts_dialog.dart';
import 'package:playground/pages/standalone_playground/widgets/editor_textarea_wrapper.dart';
import 'package:playground/pages/standalone_playground/widgets/feedback/feedback_dropdown_content.dart';
import 'package:playground/pages/standalone_playground/widgets/feedback/playground_feedback.dart';
import 'package:playground/pages/standalone_playground/widgets/more_actions.dart';
import 'package:playground_components/playground_components.dart';
import 'package:playground_components/src/widgets/drag_handle.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

extension CommonFindersExtension on CommonFinders {
  Finder codeTextAreaWrapper() {
    return byType(CodeTextAreaWrapper);
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

  Finder feedbackDropdownCancelButton() {
    return find.byKey(FeedbackDropdownContent.cancelButtonKey);
  }

  Finder feedbackDropdownContent() {
    return byType(FeedbackDropdownContent);
  }

  Finder feedbackDropdownSendButton() {
    return find.byKey(FeedbackDropdownContent.sendButtonKey);
  }

  Finder feedbackDropdownTextField() {
    return find.byKey(FeedbackDropdownContent.textFieldKey);
  }

  Finder feedbackThumbDown() {
    return find.byKey(PlaygroundFeedback.thumbDownKey);
  }

  Finder feedbackThumbUp() {
    return find.byKey(PlaygroundFeedback.thumbUpKey);
  }

  Finder moreActions() {
    return byType(MoreActions);
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

  Finder resetButton() {
    return find.byType(ResetAction);
  }

  Finder sdkItemInDropdown(Sdk sdk) {
    return find.byType(SdkSelectorRow).and(find.byKey(ValueKey(sdk)));
  }

  Finder sdkSelector() {
    return byType(SDKSelector);
  }

  Finder shortcutsModal() {
    return byType(ShortcutsDialogContent);
  }
}
