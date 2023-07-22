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

import 'package:akvelon_flutter_issue_106664_workaround/akvelon_flutter_issue_106664_workaround.dart';
import 'package:app_state/app_state.dart';
import 'package:easy_localization/easy_localization.dart';
import 'package:easy_localization_ext/easy_localization_ext.dart';
import 'package:easy_localization_loader/easy_localization_loader.dart';
import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';
import 'package:intl/intl_browser.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_strategy/url_strategy.dart';

import 'l10n/l10n.dart';
import 'locator.dart';
import 'playground_app.dart';

Future<void> main() async {
  FlutterIssue106664Workaround.instance.apply();
  setPathUrlStrategy();
  WidgetsFlutterBinding.ensureInitialized();
  await EasyLocalization.ensureInitialized();
  await PlaygroundComponents.ensureInitialized();
  await initializeServiceLocator();

  // Router API specific initialization.
  final pageStack = GetIt.instance.get<PageStack>();
  final routerDelegate = BeamRouterDelegate(pageStack);
  final parser = GetIt.instance.get<PageStackRouteInformationParser>();
  final backButtonDispatcher = PageStackBackButtonDispatcher(pageStack);

  await findSystemLocale();
  runApp(
    EasyLocalization(
      supportedLocales: L10n.locales,
      startLocale: L10n.en,
      fallbackLocale: L10n.en,
      path: 'assets/translations',
      assetLoader: MultiAssetLoader([
        PlaygroundComponents.translationLoader,
        YamlAssetLoader(),
      ]),
      child: PlaygroundApp(
        backButtonDispatcher: backButtonDispatcher,
        routerDelegate: routerDelegate,
        routeInformationParser: parser,
      ),
    ),
  );
}
