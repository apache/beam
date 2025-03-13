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
import 'package:flutter/material.dart';
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:flutter_localizations/flutter_localizations.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import 'config/locale.dart';
import 'l10n/l10n.dart';

class PlaygroundApp extends StatelessWidget {
  final BackButtonDispatcher backButtonDispatcher;
  final RouteInformationParser<Object> routeInformationParser;
  final RouterDelegate<Object> routerDelegate;

  const PlaygroundApp({
    required this.backButtonDispatcher,
    required this.routeInformationParser,
    required this.routerDelegate,
  });

  @override
  Widget build(BuildContext context) {
    return ThemeSwitchNotifierProvider(
      child: Consumer<ThemeSwitchNotifier>(
        builder: (context, themeSwitchNotifier, _) {
          return ChangeNotifierProvider<LocaleProvider>(
            create: (context) => LocaleProvider(),
            builder: (context, state) {
              final localeProvider = Provider.of<LocaleProvider>(context);
              return MaterialApp.router(
                backButtonDispatcher: backButtonDispatcher,
                routeInformationParser: routeInformationParser,
                routerDelegate: routerDelegate,
                title: 'Apache Beam Playground',
                themeMode: themeSwitchNotifier.themeMode,
                theme: kLightTheme,
                darkTheme: kDarkTheme,
                debugShowCheckedModeBanner: false,
                locale: localeProvider.locale,
                supportedLocales: L10n.locales,
                localizationsDelegates: [
                  ...context.localizationDelegates,
                  AppLocalizations.delegate,
                  GlobalMaterialLocalizations.delegate,
                  GlobalWidgetsLocalizations.delegate,
                ],
              );
            },
          );
        },
      ),
    );
  }
}
