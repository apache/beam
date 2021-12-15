import 'package:flutter_test/flutter_test.dart';
import 'package:playground/utils/run_with_retry.dart';

class ExecutionTime<T> {
  final int time;
  final T? value;
  final T? error;

  ExecutionTime(this.time, {this.value, this.error});
}

Future<ExecutionTime> withExecutionTime(Function fn) async {
  final stopwatch = Stopwatch()..start();
  try {
    final result = await fn();
    return ExecutionTime(stopwatch.elapsedMilliseconds, value: result);
  } catch (e) {
    return ExecutionTime(stopwatch.elapsedMilliseconds, error: e);
  }
}

class ResultBuilder {
  static Function getFutureWithResult(List<Future Function()> futures) {
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
