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
import 'package:google_fonts/google_fonts.dart';

import '../../constants/colors.dart';
import '../../constants/sizes.dart';

final kLightTheme = ThemeData(
  brightness: Brightness.light,
  primaryColor: TobLightThemeColors.primary,
  canvasColor: TobLightThemeColors.primaryBackground,
  scaffoldBackgroundColor: TobLightThemeColors.secondaryBackground,
  backgroundColor: TobLightThemeColors.primaryBackground,
  textTheme: _getTextTheme(TobLightThemeColors.text),
  textButtonTheme: _getTextButtonTheme(TobLightThemeColors.text),
  outlinedButtonTheme: _getOutlineButtonTheme(
    TobLightThemeColors.text,
    TobLightThemeColors.primary,
  ),
  elevatedButtonTheme: _getElevatedButtonTheme(TobLightThemeColors.primary),
  appBarTheme: _getAppBarTheme(TobLightThemeColors.secondaryBackground),
);

final kDarkTheme = ThemeData(
  brightness: Brightness.dark,
  primaryColor: TobDarkThemeColors.primary,
  canvasColor: TobDarkThemeColors.primaryBackground,
  scaffoldBackgroundColor: TobDarkThemeColors.secondaryBackground,
  backgroundColor: TobDarkThemeColors.primaryBackground,
  textTheme: _getTextTheme(TobDarkThemeColors.text),
  textButtonTheme: _getTextButtonTheme(TobDarkThemeColors.text),
  outlinedButtonTheme: _getOutlineButtonTheme(
    TobDarkThemeColors.text,
    TobDarkThemeColors.primary,
  ),
  elevatedButtonTheme: _getElevatedButtonTheme(TobDarkThemeColors.primary),
  appBarTheme: _getAppBarTheme(TobDarkThemeColors.secondaryBackground),
);

TextTheme _getTextTheme(Color textColor) {
  return GoogleFonts.sourceSansProTextTheme(
    const TextTheme(
      displayLarge: _emptyTextStyle,
      displayMedium: TextStyle(
        fontSize: 48,
        fontWeight: FontWeight.w900,
      ),
      displaySmall: TextStyle(
        fontFamily: 'Roboto_regular',
        fontSize: 18,
        fontWeight: FontWeight.w400,
      ),
      headlineLarge: _emptyTextStyle,
      headlineMedium: _emptyTextStyle,
      headlineSmall: TextStyle(
        fontSize: 12,
        fontWeight: FontWeight.w600,
      ),
      titleLarge: TextStyle(
        fontSize: 24,
        fontWeight: FontWeight.w600,
      ),
      titleMedium: _emptyTextStyle,
      titleSmall: _emptyTextStyle,
      labelLarge: TextStyle(
        fontSize: 16,
        fontWeight: FontWeight.w600,
      ),
      labelMedium: _emptyTextStyle,
      labelSmall: _emptyTextStyle,
      bodyLarge: TextStyle(
        fontSize: 24,
        fontWeight: FontWeight.w400,
      ),
      bodyMedium: TextStyle(
        fontSize: 13,
        fontWeight: FontWeight.w400,
      ),
      bodySmall: _emptyTextStyle,
    ).apply(
      bodyColor: textColor,
      displayColor: textColor,
    ),
  );
}

TextButtonThemeData _getTextButtonTheme(Color textColor) {
  return TextButtonThemeData(
    style: TextButton.styleFrom(
      primary: textColor,
      shape: _getButtonBorder(TobBorderRadius.large),
    ),
  );
}

OutlinedButtonThemeData _getOutlineButtonTheme(
  Color textColor,
  Color outlineColor,
) {
  return OutlinedButtonThemeData(
    style: OutlinedButton.styleFrom(
      primary: textColor,
      side: BorderSide(color: outlineColor, width: 3),
      padding: _buttonPadding,
      shape: _getButtonBorder(TobBorderRadius.small),
    ),
  );
}

ElevatedButtonThemeData _getElevatedButtonTheme(Color color) {
  return ElevatedButtonThemeData(
    style: ElevatedButton.styleFrom(
      onPrimary: TobColors.white,
      primary: color,
      padding: _buttonPadding,
      elevation: TobSizes.size0,
    ),
  );
}

AppBarTheme _getAppBarTheme(Color backgroundColor) {
  return AppBarTheme(
    color: backgroundColor,
    elevation: TobSizes.size1,
    centerTitle: false,
    toolbarHeight: TobSizes.appBarHeight,
  );
}

const EdgeInsets _buttonPadding = EdgeInsets.symmetric(
  vertical: TobSizes.size20,
  horizontal: TobSizes.size40,
);

RoundedRectangleBorder _getButtonBorder(double radius) {
  return RoundedRectangleBorder(
    borderRadius: BorderRadius.all(
      Radius.circular(radius),
    ),
  );
}

const TextStyle _emptyTextStyle = TextStyle();
