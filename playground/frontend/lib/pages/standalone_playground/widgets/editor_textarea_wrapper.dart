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
import '../../../modules/examples/components/multifile_popover/multifile_popover_button.dart';

/// A code editor with controls stacked above it.
class CodeTextAreaWrapper extends StatelessWidget {
  final PlaygroundController controller;

  const CodeTextAreaWrapper({
    required this.controller,
  });

  @override
  Widget build(BuildContext context) {
    if (controller.result?.errorMessage?.isNotEmpty ?? false) {
      WidgetsBinding.instance.addPostFrameCallback((_) {
        _handleError(context, controller);
      });
    }

    final snippetController = controller.snippetEditingController;

    if (snippetController == null) {
      return const LoadingIndicator();
    }

    return Column(
      children: [
        Expanded(
          child: Stack(
            children: [
              Positioned.fill(
                child: SnippetEditor(
                  controller: snippetController,
                  isEditable: true,
                ),
              ),
              Positioned(
                right: kXlSpacing,
                top: kXlSpacing,
                height: kButtonHeight,
                child: Row(
                  children: [
                    if (controller.selectedExample != null) ...[
                      if (controller.selectedExample?.isMultiFile ?? false)
                        Semantics(
                          container: true,
                          child: MultifilePopoverButton(
                            example: controller.selectedExample!,
                            followerAnchor: Alignment.topRight,
                            targetAnchor: Alignment.bottomRight,
                          ),
                        ),
                      Semantics(
                        container: true,
                        child: DescriptionPopoverButton(
                          example: controller.selectedExample!,
                          followerAnchor: Alignment.topRight,
                          targetAnchor: Alignment.bottomRight,
                        ),
                      ),
                    ],
                    Semantics(
                      container: true,
                      child: ShareButton(
                        playgroundController: controller,
                      ),
                    ),
                    const SizedBox(width: kLgSpacing),
                    Semantics(
                      container: true,
                      child: const PlaygroundRunOrCancelButton(),
                    ),
                  ],
                ),
              ),
            ],
          ),
        ),
      ],
    );
  }

  void _handleError(BuildContext context, PlaygroundController controller) {
    PlaygroundComponents.toastNotifier.add(
      Toast(
        description: controller.result?.errorMessage ?? '',
        title: AppLocalizations.of(context)!.runCode,
        type: ToastType.error,
      ),
    );
    controller.resetError();
  }
}
