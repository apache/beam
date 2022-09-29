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

import 'package:playground/modules/messages/handlers/abstract_message_handler.dart';
import 'package:playground/modules/messages/handlers/set_content_message_handler.dart';
import 'package:playground/modules/messages/handlers/set_sdk_message_handler.dart';
import 'package:playground/modules/messages/models/abstract_message.dart';
import 'package:playground_components/playground_components.dart';

class MessagesHandler extends AbstractMessageHandler {
  final List<AbstractMessageHandler> handlers;

  MessagesHandler({
    required PlaygroundController playgroundController,
  }) : handlers = [
    SetContentMessageHandler(playgroundController: playgroundController),
    SetSdkMessageHandler(playgroundController: playgroundController),
  ];

  @override
  MessageHandleResult handle(AbstractMessage message) {
    for (final handler in handlers) {
      final result = handler.handle(message);

      if (result == MessageHandleResult.handled) {
        return MessageHandleResult.handled;
      }
    }

    return MessageHandleResult.notHandled;
  }
}
