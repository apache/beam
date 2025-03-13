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

import 'package:playground/constants/params.dart';
import 'package:playground/modules/messages/handlers/abstract_message_handler.dart';
import 'package:playground/modules/messages/models/abstract_message.dart';
import 'package:playground/modules/messages/models/set_content_message.dart';
import 'package:playground_components/playground_components.dart';

/// A handler for [SetContentMessage].
class SetContentMessageHandler extends AbstractMessageHandler {
  final PlaygroundController playgroundController;

  const SetContentMessageHandler({
    required this.playgroundController,
  });

  @override
  MessageHandleResult handle(AbstractMessage message) {
    if (message is! SetContentMessage) {
      return MessageHandleResult.notHandled;
    }

    unawaited(_handle(message));
    return MessageHandleResult.handled;
  }

  Future<void> _handle(SetContentMessage message) async {
    final descriptor = message.descriptor;

    try {
      await playgroundController.examplesLoader.loadIfNew(descriptor);
    } on Exception catch (ex) {
      PlaygroundComponents.toastNotifier.addException(ex);

      playgroundController.setEmptyIfNoSdk(
        descriptor.initialSdk ?? defaultSdk,
      );
    }
  }
}
