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
import 'package:easy_localization/easy_localization.dart';
import 'package:easy_localization_ext/easy_localization_ext.dart';
import 'package:easy_localization_loader/easy_localization_loader.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';
import 'package:url_strategy/url_strategy.dart';

import 'firebase_options.dart';
import 'locator.dart';
import 'router/route_information_parser.dart';

Future<void> main() async {
  await Firebase.initializeApp(
    options: DefaultFirebaseOptions.currentPlatform,
  );
  setPathUrlStrategy();
  await EasyLocalization.ensureInitialized();
  await PlaygroundComponents.ensureInitialized();
  await initializeServiceLocator();
  const englishLocale = Locale('en');

  final pageStack = GetIt.instance.get<PageStack>();
  final routerDelegate = GetIt.instance.get<BeamRouterDelegate>();
  final routeInformationParser = TobRouteInformationParser();
  final backButtonDispatcher = PageStackBackButtonDispatcher(pageStack);

  PlaygroundComponents.analyticsService.defaultEventParameters = {
    EventParams.app: 'tob',
  };

  runApp(
    EasyLocalization(
      supportedLocales: const [englishLocale],
      startLocale: englishLocale,
      fallbackLocale: englishLocale,
      path: 'assets/translations',
      assetLoader: MultiAssetLoader([
        PlaygroundComponents.translationLoader,
        YamlAssetLoader(),
      ]),
      child: TourOfBeamApp(
        backButtonDispatcher: backButtonDispatcher,
        routerDelegate: routerDelegate,
        routeInformationParser: routeInformationParser,
      ),
    ),
  );
}

class TourOfBeamApp extends StatelessWidget {
  final BackButtonDispatcher backButtonDispatcher;
  final PageStackRouteInformationParser routeInformationParser;
  final PageStackRouterDelegate routerDelegate;

  const TourOfBeamApp({
    required this.backButtonDispatcher,
    required this.routeInformationParser,
    required this.routerDelegate,
  });

  @override
  Widget build(BuildContext context) {
    return ThemeSwitchNotifierProvider(
      child: Consumer<ThemeSwitchNotifier>(
        builder: (context, themeSwitchNotifier, _) {
          return MaterialApp.router(
            backButtonDispatcher: backButtonDispatcher,
            routeInformationParser: routeInformationParser,
            routerDelegate: routerDelegate,
            debugShowCheckedModeBanner: false,
            themeMode: themeSwitchNotifier.themeMode,
            theme: kLightTheme,
            darkTheme: kDarkTheme,
            localizationsDelegates: context.localizationDelegates,
            supportedLocales: context.supportedLocales,
            locale: context.locale,
            title: 'Tour of Beam',
          );
        },
      ),
    );
  }
}
