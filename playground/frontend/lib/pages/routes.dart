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

import 'package:flutter/material.dart';
import 'package:playground/constants/params.dart';
import 'package:playground/pages/embedded_playground/embedded_playground_page.dart';
import 'package:playground/pages/playground/playground_page.dart';

class Routes {
  static const String embedded = '/embedded';

  static Route<dynamic> generateRoute(RouteSettings settings) {
    final name = settings.name ?? '';
    final queryIndex = name.indexOf('?');
    final routePath =
        name.substring(0, queryIndex < 0 ? name.length : queryIndex);

    switch (routePath) {
      case Routes.embedded:
        final isEditable = Uri.base.queryParameters[kIsEditableParam] == '1';

        return _renderRoute(
          EmbeddedPlaygroundPage(
            isEditable: isEditable,
          ),
        );

      default:
        return _renderRoute(const PlaygroundPage());
    }
  }

  static _renderRoute(Widget widget) {
    return MaterialPageRoute(builder: (context) => widget);
  }
}
