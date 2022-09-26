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

// ignore_for_file: prefer_const_constructors

import 'package:fake_async/fake_async.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/modules/messages/handlers/abstract_message_handler.dart';
import 'package:playground/modules/messages/handlers/messages_debouncer.dart';
import 'package:playground/modules/messages/models/abstract_message.dart';
import 'package:playground/modules/messages/models/set_sdk_message.dart';
import 'package:playground_components/playground_components.dart';

void main() {
  group('MessagesDebouncer', () {
    late _Recorder recorder;
    late MessagesDebouncer debouncer;

    setUp(() {
      recorder = _Recorder();
      debouncer = MessagesDebouncer(handler: recorder);
    });

    test('drops sequential calls, no time limit', () {
      fakeAsync((async) {
        debouncer.handle(SetSdkMessage(sdk: Sdk.java));
        debouncer.handle(SetSdkMessage(sdk: Sdk.java));
        async.elapse(const Duration(days: 36500));
        debouncer.handle(SetSdkMessage(sdk: Sdk.java));
      });

      expect(recorder.messages, [SetSdkMessage(sdk: Sdk.java)]);
    });

    test('returns the last result on debouncing', () {
      final result1 = debouncer.handle(
        _MessageWithResult(
          n: 1,
          result: MessageHandleResult.handled,
        ),
      );
      final result2 = debouncer.handle(
        _MessageWithResult(
          n: 1,
          result: MessageHandleResult.notHandled,
        ),
      );
      final result3 = debouncer.handle(
        _MessageWithResult(
          n: 2,
          result: MessageHandleResult.notHandled,
        ),
      );
      final result4 = debouncer.handle(
        _MessageWithResult(
          n: 2,
          result: MessageHandleResult.handled,
        ),
      );

      expect(result1, MessageHandleResult.handled, reason: '1');
      expect(result2, MessageHandleResult.handled, reason: '2');
      expect(result3, MessageHandleResult.notHandled, reason: '3');
      expect(result4, MessageHandleResult.notHandled, reason: '4');
    });
  });
}

class _Recorder extends AbstractMessageHandler {
  final messages = <AbstractMessage>[];

  @override
  MessageHandleResult handle(AbstractMessage message) {
    messages.add(message);

    return message is _MessageWithResult
        ? message.result
        : MessageHandleResult.handled;
  }
}

class _MessageWithResult extends AbstractMessage {
  final int n;
  final MessageHandleResult result;

  _MessageWithResult({required this.n, required this.result});

  @override
  Map<String, dynamic> toJson() => {'n': n};

  @override
  int get hashCode => n.hashCode;

  // Disregard `result` for comparison, this is the point of the test.
  @override
  bool operator ==(Object other) => other is _MessageWithResult && other.n == n;
}
