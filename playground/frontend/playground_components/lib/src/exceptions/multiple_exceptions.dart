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

import 'detailed_exception.dart';

class MultipleExceptions implements DetailedException {
  final String message;
  final List<Exception> exceptions;
  final List<StackTrace> stackTraces;

  MultipleExceptions(this.message, {
    required this.exceptions,
    required this.stackTraces,
  });

  @override
  String toString() => message;

  @override
  String get details {
    final buffer = StringBuffer('Exceptions (${exceptions.length}): ');
    for (var i = 0; i < exceptions.length; i++) {
      buffer
        ..write('Exception #')
        ..write(i + 1)
        ..writeln(':')
        ..writeln(exceptions[i])
        ..writeln('StackTrace:')
        ..writeln(stackTraces[i]);
    }
    return buffer.toString();
  }
}
