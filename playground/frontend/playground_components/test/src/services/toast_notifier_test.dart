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
import 'package:playground_components/playground_components.dart';
import 'package:playground_components/src/services/toast_notifier.dart';

void main() {
  late ToastNotifier notifier;
  final toasts = <Toast>[];

  setUp((){
    toasts.clear();
    notifier = ToastNotifier();
    notifier.toasts.listen(toasts.add);
  });

  group('ToastNotifier', () {
    test('add', () async {
      const toast = Toast(
        title: 'title',
        description: 'text',
        type: ToastType.info,
      );

      notifier.add(toast);
      await Future.delayed(Duration.zero);

      expect(toasts, [toast]);
    });

    test('addException', () async {
      final exception = _TestException();

      notifier.addException(exception);
      await Future.delayed(Duration.zero);

      expect(
        toasts,
        [
          Toast(
            title: 'errors.error',
            description: exception.toString(),
            type: ToastType.error,
          ),
        ],
      );
    });
  });
}

class _TestException implements Exception {
  @override
  String toString() => 'test';
}
