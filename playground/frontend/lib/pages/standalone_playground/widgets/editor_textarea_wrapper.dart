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
import 'package:playground_components/playground_components.dart';

import '../../../components/playground_run_or_cancel_button.dart';
import '../../../constants/sizes.dart';
import '../../../modules/editor/components/share_dropdown/share_button.dart';
import '../../../modules/examples/components/description_popover/description_popover_button.dart';

/// A code editor with controls stacked above it.
class CodeTextAreaWrapper extends StatelessWidget {
  final PlaygroundController playgroundController;

  const CodeTextAreaWrapper({
    required this.playgroundController,
  });

  @override
  Widget build(BuildContext context) {
    if (playgroundController.codeRunner.result?.errorMessage?.isNotEmpty ??
        false) {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        _handleError(context, playgroundController);
      });
    }

    final snippetController = playgroundController.snippetEditingController;

    if (snippetController == null) {
      return const LoadingIndicator();
    }

    final example = snippetController.example;

    return SnippetEditor(
      autofocus: true,
      controller: snippetController,
      isEditable: true,
      actionsWidget: Row(
        children: [
          if (example != null)
            Semantics(
              container: true,
              child: DescriptionPopoverButton(
                example: example,
                followerAnchor: Alignment.topRight,
                targetAnchor: Alignment.bottomRight,
              ),
            ),
          Semantics(
            container: true,
            child: ShareButton(
              playgroundController: playgroundController,
            ),
          ),
          const SizedBox(width: kLgSpacing),
          Semantics(
            container: true,
            child: const PlaygroundRunOrCancelButton(),
          ),
          const SizedBox(width: kLgSpacing),
        ],
      ),
    );
  }

  void _handleError(BuildContext context, PlaygroundController controller) {
    //TODO(alexeyinkin): A better trigger instead of resetErrorMessageText, https://github.com/apache/beam/issues/26319
    PlaygroundComponents.toastNotifier.add(
      Toast(
        description: controller.codeRunner.result?.errorMessage ?? '',
        title: AppLocalizations.of(context)!.runCode,
        type: ToastType.error,
      ),
    );
    controller.resetErrorMessageText();
  }
}
