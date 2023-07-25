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
import 'package:playground_components/src/util/run_with_retry.dart';

class ExecutionTime<T> {
  final int time;
  final T? value;
  final Object? error;

  ExecutionTime(this.time, {this.value, this.error});
}

Future<ExecutionTime<T>> withExecutionTime<T>(Future<T> Function() fn) async {
  final stopwatch = Stopwatch()..start();
  try {
    final result = await fn();
    return ExecutionTime<T>(stopwatch.elapsedMilliseconds, value: result);
  } catch (e) {
    return ExecutionTime<T>(stopwatch.elapsedMilliseconds, error: e);
  }
}

class ResultBuilder {
  static Future<T> Function() getFutureWithResult<T>(List<Future<T> Function()> futures) {
    var attempt = 0;
    return () async {
      final futureCreator = futures[attempt];
      attempt = attempt + 1;
      return futureCreator();
    };
  }
}

void main() {
  test('runWithRetry should resolve success instantly', () async {
    final result = await withExecutionTime(
        () => runWithRetry(() => Future.value(10), retryWaitMs: 50));
    // it should run without retry, so execution time should be less than retry
    expect(result.time, lessThan(50));
    expect(result.value, equals(10));
  });

  test(
      'runWithRetry should return success result if attempts count not exceeded limits',
      () async {
    final builder = ResultBuilder.getFutureWithResult([
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.value(10),
    ]);
    final result = await withExecutionTime(
        () => runWithRetry(() => builder(), retryCount: 5, retryWaitMs: 10));
    // it should run with 4 retries, so execution time should be greater 10 * 4
    expect(result.time, greaterThan(40));
    expect(result.value, equals(10));
  });

  test(
      'runWithRetry should return error result if attempts count exceeded limits',
      () async {
    final builder = ResultBuilder.getFutureWithResult([
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.error('error'),
      () => Future.error('last error'),
    ]);
    final result = await withExecutionTime(
        () => runWithRetry(() => builder(), retryCount: 5, retryWaitMs: 10));
    // it should run with attempt + 5 retries, so execution time should be greater 10 * 5
    expect(result.time, greaterThan(50));
    expect(result.error, equals('last error'));
  });
}
