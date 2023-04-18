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

import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/src/controllers/unread_controller.dart';

void main() {
  UnreadController<String> controller = UnreadController();
  int notified = 0;

  setUp(() {
    controller = UnreadController();
    notified = 0;

    controller.addListener(() {
      notified++;
    });
  });

  group('UnreadController', () {
    test('setValue, isUnread, clearKey, clear', () {
      controller.setValue('a', 1);
      expect(controller.isUnread('a'), true);
      expect(controller.isUnread('b'), false);
      expect(notified, 1);

      controller.setValue('a', 1);
      expect(controller.isUnread('a'), true);
      expect(controller.isUnread('b'), false);
      expect(notified, 1);

      controller.setValue('a', 2);
      expect(controller.isUnread('a'), true);
      expect(controller.isUnread('b'), false);
      expect(notified, 2);

      controller.setValue('b', 1);
      expect(controller.isUnread('a'), true);
      expect(controller.isUnread('b'), true);
      expect(notified, 3);

      controller.markRead('b');
      expect(controller.isUnread('a'), true);
      expect(controller.isUnread('b'), false);
      expect(notified, 4);

      controller.setValue('b', 1);
      expect(controller.isUnread('a'), true);
      expect(controller.isUnread('b'), false);
      expect(notified, 4);

      controller.setValue('c', 1);
      controller.markAllRead();
      expect(controller.isUnread('a'), false);
      expect(controller.isUnread('c'), false);
      expect(notified, 6);

      controller.markAllRead();
      expect(notified, 6);
    });
  });
}
