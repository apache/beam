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

import 'package:app_state/app_state.dart';
import 'package:flutter/widgets.dart';

import '../welcome/path.dart';
import 'page.dart';

class TourPath extends PagePath {
  final String sdkId;
  final List<String> breadcrumbIds;

  static final _regExp =
      RegExp(r'^/tour/([a-z]+)/?([-a-zA-Z0-9]+(/[-a-zA-Z0-9]+)*)?/?$');

  TourPath({
    required this.sdkId,
    this.breadcrumbIds = const [],
  }) : super(
          key: TourPage.classFactoryKey,
          state: {
            'sdkId': sdkId,
            'breadcrumbIds': breadcrumbIds,
          },
        );

  @override
  String get location => ['/tour/$sdkId', ...breadcrumbIds].join('/');

  static TourPath? tryParse(RouteInformation ri) {
    final matches = _regExp.firstMatch(ri.location ?? '');
    if (matches == null) return null;

    final sdkId = matches[1] ?? (throw Error());
    final breadcrumbIdsString = matches[2];

    final breadcrumbIds = breadcrumbIdsString == null
        ? const <String>[]
        : breadcrumbIdsString.split('/');

    return TourPath(
      sdkId: sdkId,
      breadcrumbIds: breadcrumbIds,
    );
  }

  @override
  List<PagePath> get defaultStackPaths => [
        const WelcomePath(),
        this,
      ];
}
