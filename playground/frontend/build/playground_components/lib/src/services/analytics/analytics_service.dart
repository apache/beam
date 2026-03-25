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
import 'dart:collection';

import 'package:get_it/get_it.dart';
import 'package:meta/meta.dart';

import 'events/abstract.dart';

/// Abstract class to submit analytics events.
abstract class BeamAnalyticsService {
  AnalyticsEvent? _lastEvent;
  AnalyticsEvent? get lastEvent => _lastEvent;

  final _defaultEventParameters = <String, dynamic>{};

  /// The parameters sent with all events.
  ///
  /// Individual event parameters have higher priority on collisions.
  Map<String, dynamic> get defaultEventParameters =>
      UnmodifiableMapView(_defaultEventParameters);

  set defaultEventParameters(Map<String, dynamic> newValue) {
    _defaultEventParameters.clear();
    _defaultEventParameters.addAll(newValue);
  }

  static BeamAnalyticsService get() {
    return GetIt.instance.get<BeamAnalyticsService>();
  }

  /// Sends [event] asynchronously without returning a [Future]
  /// to avoid `discarded_futures` lint.
  void sendUnawaited(AnalyticsEvent event) {
    unawaited(_send(event));
  }

  Future<void> _send(AnalyticsEvent event) async {
    _lastEvent = event;
    await sendProtected(event);
  }

  @protected
  Future<void> sendProtected(AnalyticsEvent event);
}
