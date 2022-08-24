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

import 'dart:convert';

import 'package:playground/constants/params.dart';

enum PlaygroundView {
  standalone,
  embedded,
  ;

  String get path {
    switch (this) {
      case PlaygroundView.standalone:
        return '/';
      case PlaygroundView.embedded:
        return '/embedded';
    }
  }
}

extension CopyWith on Uri {
  Uri copyWith({
    String? path,
    Map<String, dynamic>? queryParameters,
  }) {
    return Uri(
      scheme: scheme,
      userInfo: userInfo,
      host: host,
      port: port,
      path: path ?? this.path,
      queryParameters: queryParameters ?? this.queryParameters,
    );
  }
}

class ShareCodeUtils {
  static const _width = '90%';
  static const _height = '600px';

  static Uri examplePathToPlaygroundUrl({
    required String examplePath,
    required PlaygroundView view,
  }) {
    return Uri.base.copyWith(
      path: view.path,
      queryParameters: _getExampleQueryParameters(
        examplePath: examplePath,
        view: view,
      ),
    );
  }

  static Map<String, dynamic> _getExampleQueryParameters({
    required String examplePath,
    required PlaygroundView view,
  }) {
    switch (view) {
      case PlaygroundView.standalone:
        return {
          kExampleParam: examplePath,
        };
      case PlaygroundView.embedded:
        return {
          kIsEditableParam: '1',
          kExampleParam: examplePath,
        };
    }
  }

  static String examplePathToIframeCode({
    required String examplePath,
  }) {
    return _iframe(
      src: examplePathToPlaygroundUrl(
        examplePath: examplePath,
        view: PlaygroundView.embedded,
      ),
    );
  }

  static String _iframe({
    required Uri src,
  }) {
    return '<iframe'
        ' src="${Uri.encodeComponent(src.toString())}"'
        ' width="${_htmlEscape(_width)}"'
        ' height="${_htmlEscape(_height)}"'
        ' allow="clipboard-write" '
        '></iframe>';
  }

  static String _htmlEscape(String text) => const HtmlEscape().convert(text);
}
