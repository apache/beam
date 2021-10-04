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
import 'package:playground/constants/colors.dart';
import 'package:playground/constants/sizes.dart';
import 'package:provider/provider.dart';

class ThemeProvider extends ChangeNotifier {
  ThemeMode themeMode = ThemeMode.light;

  bool get isDarkMode {
    return themeMode == ThemeMode.dark;
  }

  void toggleTheme() {
    themeMode = themeMode == ThemeMode.light ? ThemeMode.dark : ThemeMode.light;
    notifyListeners();
  }
}

final kLightTheme = ThemeData(
  brightness: Brightness.light,
  primaryColor: kLightPrimary,
  backgroundColor: kLightPrimaryBackground,
  appBarTheme: const AppBarTheme(
    color: kLightSecondaryBackground,
    elevation: 1,
    centerTitle: false,
  ),
  textButtonTheme: TextButtonThemeData(
    style: TextButton.styleFrom(
      primary: kLightText,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(kBorderRadius)),
      ),
    ),
  ),
);

final kDarkTheme = ThemeData(
  brightness: Brightness.dark,
  primaryColor: kDarkPrimary,
  backgroundColor: kDarkGrey,
  appBarTheme: const AppBarTheme(
    color: kDarkSecondaryBackground,
    elevation: 1,
    centerTitle: false,
  ),
  textButtonTheme: TextButtonThemeData(
    style: TextButton.styleFrom(
      primary: kDarkText,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(kBorderRadius)),
      ),
    ),
  ),
);

class ThemeColors {
  final bool isDark;

  static ThemeColors of(BuildContext context) {
    final theme = Provider.of<ThemeProvider>(context);
    return ThemeColors(theme.isDarkMode);
  }

  ThemeColors(this.isDark);

  Color get greyColor => isDark ? kDarkGrey : kLightGrey;
}
