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
import 'dart:convert';
import 'dart:html'; // ignore: avoid_web_libraries_in_flutter
import 'dart:js' as js; // ignore: avoid_web_libraries_in_flutter

import 'package:meta/meta.dart';

import '../events/abstract.dart';
import 'google_analytics4_service.dart';

final _urlTemplate = Uri.parse('https://www.googletagmanager.com/gtag/js');
const _eventNameParam = 'eventName';

/// The global JS function to submit analytics data.
const _function = 'gtag';

@internal
BeamGoogleAnalytics4Service createGoogleAnalytics4Service({
  required String measurementId,
}) =>
    BeamGoogleAnalytics4ServiceWeb(measurementId: measurementId);

/// Submits data to a Google Analytics 4 property using JavaScript.
class BeamGoogleAnalytics4ServiceWeb extends BeamGoogleAnalytics4Service {
  final String measurementId;
  final _readyCompleter = Completer<void>();

  BeamGoogleAnalytics4ServiceWeb({
    required this.measurementId,
  }) : super.create() {
    _loadGoogleJs();
  }

  void _loadGoogleJs() {
    // Replicating the JS from the installation manual for websites.
    _evalJs('window.dataLayer = window.dataLayer || [];');
    _evalJs('window.$_function = function () { dataLayer.push(arguments); }');
    _logJsDate();
    _logConfig();

    final url = _urlTemplate.replace(queryParameters: {'id': measurementId});
    final element = document.createElement('script') as ScriptElement;
    element.async = true;
    element.src = url.toString(); // ignore: unsafe_html
    element.onLoad.listen(_readyCompleter.complete);
    document.head!.append(element);
  }

  static dynamic _evalJs(String code) {
    print('JS eval: $code'); // ignore: avoid_print
    return js.context.callMethod('eval', [code]);
  }

  void _logJsDate() {
    _logEncoded('js', 'new Date()');
  }

  void _logConfig() {
    _log('config', [measurementId]);
  }

  void _log(String command, List<Object?> arguments) {
    _logEncoded(command, arguments.map(jsonEncode).join(','));
  }

  void _logEncoded(String command, String arguments) {
    _evalJs('$_function(${jsonEncode(command)}, $arguments)');
  }

  @override
  Future<void> sendProtected(AnalyticsEvent event) async {
    await _readyCompleter.future;

    // Google Analytics cannot use event names as a dimension,
    // so also add the event name as a parameter.
    final params = {
      ...defaultEventParameters,
      _eventNameParam: event.name,
      ...event.toJson(),
    };

    _log('event', [event.name, params]);
  }
}
