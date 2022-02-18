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
import 'package:playground/constants/font_weight.dart';
import 'package:playground/constants/fonts.dart';
import 'package:playground/constants/sizes.dart';
import 'package:provider/provider.dart';
import 'package:shared_preferences/shared_preferences.dart';

const kThemeMode = 'theme_mode';

class ThemeProvider extends ChangeNotifier {
  late SharedPreferences _preferences;
  ThemeMode themeMode = ThemeMode.light;

  init() {
    _setPreferences();
  }

  _setPreferences() async {
    _preferences = await SharedPreferences.getInstance();
    themeMode = _preferences.getString(kThemeMode) == ThemeMode.dark.toString()
        ? ThemeMode.dark
        : ThemeMode.light;
    notifyListeners();
  }

  bool get isDarkMode {
    return themeMode == ThemeMode.dark;
  }

  void toggleTheme() {
    themeMode = themeMode == ThemeMode.light ? ThemeMode.dark : ThemeMode.light;
    _preferences.setString(kThemeMode, themeMode.toString());
    notifyListeners();
  }
}

TextTheme createTextTheme(Color textColor) {
  return getBaseFontTheme(
    const TextTheme(
      headline1: TextStyle(),
      headline2: TextStyle(),
      headline3: TextStyle(),
      headline4: TextStyle(),
      headline5: TextStyle(),
      headline6: TextStyle(),
      subtitle1: TextStyle(),
      subtitle2: TextStyle(),
      bodyText1: TextStyle(),
      bodyText2: TextStyle(),
      caption: TextStyle(),
      overline: TextStyle(),
      button: TextStyle(fontWeight: kBoldWeight),
    ).apply(
      bodyColor: textColor,
      displayColor: textColor,
    ),
  );
}

TextButtonThemeData createTextButtonTheme(Color textColor) {
  return TextButtonThemeData(
    style: TextButton.styleFrom(
      primary: textColor,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(kLgBorderRadius)),
      ),
    ),
  );
}

OutlinedButtonThemeData createOutlineButtonTheme(Color textColor) {
  return OutlinedButtonThemeData(
    style: OutlinedButton.styleFrom(
      primary: textColor,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.all(Radius.circular(kSmBorderRadius)),
      ),
    ),
  );
}

ElevatedButtonThemeData createElevatedButtonTheme(Color primaryColor) {
  return ElevatedButtonThemeData(
    style: ElevatedButton.styleFrom(primary: primaryColor),
  );
}

PopupMenuThemeData createPopupMenuTheme() {
  return const PopupMenuThemeData(
    shape: RoundedRectangleBorder(
      borderRadius: BorderRadius.all(
        Radius.circular(kLgBorderRadius),
      ),
    ),
  );
}

AppBarTheme createAppBarTheme(Color backgroundColor) {
  return AppBarTheme(
    color: backgroundColor,
    elevation: 1,
    centerTitle: false,
  );
}

TabBarTheme createTabBarTheme(Color textColor, Color indicatorColor) {
  const labelStyle = TextStyle(fontWeight: kMediumWeight);
  return TabBarTheme(
    unselectedLabelColor: textColor,
    labelColor: textColor,
    labelStyle: labelStyle,
    unselectedLabelStyle: labelStyle,
    indicator: UnderlineTabIndicator(
      borderSide: BorderSide(width: 2.0, color: indicatorColor),
    ),
  );
}

DialogTheme createDialogTheme(Color textColor) {
  return DialogTheme(
    titleTextStyle: TextStyle(
      color: textColor,
      fontSize: 32.0,
      fontWeight: kBoldWeight,
    ),
  );
}

final kLightTheme = ThemeData(
  brightness: Brightness.light,
  primaryColor: kLightPrimary,
  backgroundColor: kLightPrimaryBackground,
  appBarTheme: createAppBarTheme(kLightSecondaryBackground),
  textTheme: createTextTheme(kLightText),
  popupMenuTheme: createPopupMenuTheme(),
  textButtonTheme: createTextButtonTheme(kLightText),
  outlinedButtonTheme: createOutlineButtonTheme(kLightText),
  elevatedButtonTheme: createElevatedButtonTheme(kLightPrimary),
  tabBarTheme: createTabBarTheme(kLightText, kLightPrimary),
  dialogTheme: createDialogTheme(kLightText),
);

final kDarkTheme = ThemeData(
  brightness: Brightness.dark,
  primaryColor: kDarkPrimary,
  backgroundColor: kDarkPrimaryBackground,
  appBarTheme: createAppBarTheme(kDarkSecondaryBackground),
  textTheme: createTextTheme(kDarkText),
  popupMenuTheme: createPopupMenuTheme(),
  textButtonTheme: createTextButtonTheme(kDarkText),
  outlinedButtonTheme: createOutlineButtonTheme(kDarkText),
  elevatedButtonTheme: createElevatedButtonTheme(kDarkPrimary),
  tabBarTheme: createTabBarTheme(kDarkText, kDarkPrimary),
  dialogTheme: createDialogTheme(kDarkText),
);

class ThemeColors {
  final bool isDark;

  static ThemeColors of(BuildContext context) {
    final theme = Provider.of<ThemeProvider>(context);
    return ThemeColors(theme.isDarkMode);
  }

  ThemeColors(this.isDark);

  Color get greyColor => isDark ? kDarkGrey : kLightGrey;

  Color get lightGreyColor => isDark ? kLightGrey1 : kLightGrey;

  Color get primary => isDark ? kLightPrimary : kDarkPrimary;

  Color get primaryBackgroundTextColor => Colors.white;

  Color get lightGreyBackgroundTextColor => Colors.black;

  Color get grey1Color => isDark ? kDarkGrey1 : kLightGrey1;

  Color get secondaryBackground =>
      isDark ? kDarkSecondaryBackground : kLightSecondaryBackground;

  Color get primaryBackground =>
      isDark ? kDarkPrimaryBackground : kLightPrimaryBackground;

  Color get code1 => isDark ? kDarkCode2 : kLightCode2;

  Color get code2 => isDark ? kDarkCode1 : kLightCode1;

  Color get codeComment => isDark ? kDarkCodeComment : kLightCodeComment;

  Color get textColor => isDark ? kDarkText : kLightText;
}
