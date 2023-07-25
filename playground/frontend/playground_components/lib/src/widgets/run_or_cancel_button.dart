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

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/widgets.dart';

import '../controllers/code_runner.dart';
import '../controllers/playground_controller.dart';
import '../models/toast.dart';
import '../models/toast_type.dart';
import '../playground_components.dart';
import '../repositories/models/run_code_result.dart';
import 'run_button.dart';

class RunOrCancelButton extends StatelessWidget {
  final ValueChanged<CodeRunner>? beforeCancel;
  final VoidCallback? beforeRun;
  final ValueChanged<CodeRunner>? onComplete;
  final PlaygroundController playgroundController;

  const RunOrCancelButton({
    required this.playgroundController,
    this.beforeCancel,
    this.beforeRun,
    this.onComplete,
  });

  @override
  Widget build(BuildContext context) {
    return RunButton(
      playgroundController: playgroundController,
      cancelRun: () async {
        beforeCancel?.call(playgroundController.codeRunner);
        await playgroundController.codeRunner.cancelRun().catchError(
              (_) => PlaygroundComponents.toastNotifier.add(_getErrorToast()),
            );
      },
      runCode: () async {
        beforeRun?.call();
        final runner = playgroundController.codeRunner;
        await runner.runCode();
        if (runner.result?.status == RunCodeStatus.finished) {
          onComplete?.call(playgroundController.codeRunner);
        }
      },
    );
  }

  Toast _getErrorToast() {
    return Toast(
      title: 'widgets.runOrCancelButton.notificationTitles.runCode'.tr(),
      description:
          'widgets.runOrCancelButton.notificationTitles.cancelExecution'.tr(),
      type: ToastType.error,
    );
  }
}
