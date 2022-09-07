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
import 'package:playground/modules/messages/models/abstract_message.dart';

/// Drops messages that repeat the last one. Has no time limit.
class MessagesDebouncer extends AbstractMessageHandler {
  final AbstractMessageHandler handler;
  AbstractMessage? _lastMessage;
  MessageHandleResult _lastResult = MessageHandleResult.notHandled;

  MessagesDebouncer({
    required this.handler,
  });

  @override
  MessageHandleResult handle(AbstractMessage message) {
    if (message == _lastMessage) {
      return _lastResult;
    }

    _lastMessage = message;
    _lastResult = handler.handle(message);
    return _lastResult;
  }
}
