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

import 'package:easy_localization/easy_localization.dart';
import 'package:easy_localization_ext/easy_localization_ext.dart';
import 'package:easy_localization_loader/easy_localization_loader.dart';
import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';
import 'package:url_strategy/url_strategy.dart';

import 'locator.dart';
import 'pages/tour/screen.dart';

void main() async {
  setPathUrlStrategy();
  await EasyLocalization.ensureInitialized();
  await initializeServiceLocator();
  const englishLocale = Locale('en');

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
      child: const TourOfBeamApp(),
    ),
  );
}

class TourOfBeamApp extends StatelessWidget {
  const TourOfBeamApp();

  @override
  Widget build(BuildContext context) {
    return ThemeSwitchNotifierProvider(
      child: Consumer<ThemeSwitchNotifier>(
        builder: (context, themeSwitchNotifier, _) {
          return MaterialApp(
            debugShowCheckedModeBanner: false,
            themeMode: themeSwitchNotifier.themeMode,
            theme: kLightTheme,
            darkTheme: kDarkTheme,
            localizationsDelegates: context.localizationDelegates,
            supportedLocales: context.supportedLocales,
            locale: context.locale,
            home: const TourScreen(),
          );
        },
      ),
    );
  }
}
