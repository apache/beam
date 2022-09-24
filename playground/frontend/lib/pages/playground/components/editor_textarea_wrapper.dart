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
import 'package:playground/components/playground_run_or_cancel_button.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/editor/components/share_dropdown/share_button.dart';
import 'package:playground/modules/examples/components/description_popover/description_popover_button.dart';
import 'package:playground/modules/examples/components/multifile_popover/multifile_popover_button.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

class CodeTextAreaWrapper extends StatelessWidget {
  const CodeTextAreaWrapper({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundController>(builder: (context, controller, child) {
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
                    goToContextLine: true,
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
                        child: const ShareButton(),
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
    });
  }

  void _handleError(BuildContext context, PlaygroundController controller) {
    NotificationManager.showError(
      context,
      AppLocalizations.of(context)!.runCode,
      controller.result?.errorMessage ?? '',
    );
    controller.resetError();
  }
}
