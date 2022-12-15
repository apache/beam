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
import 'package:flutter_code_editor/flutter_code_editor.dart';
import 'package:flutter_markdown/flutter_markdown.dart';
import 'package:google_fonts/google_fonts.dart';

import '../../playground_components.dart';

const codeFontSize = 14.0;

class BeamThemeExtension extends ThemeExtension<BeamThemeExtension> {
  final Color borderColor;
  final Color fieldBackgroundColor;
  final Color iconColor;
  final Color primaryBackgroundTextColor;
  final Color lightGreyBackgroundTextColor;
  final Color secondaryBackgroundColor;
  // TODO(nausharipov): simplify new color addition
  final Color selectedProgressColor;
  final Color unselectedProgressColor;

  final Color codeBackgroundColor;
  final TextStyle codeRootStyle;
  final CodeThemeData codeTheme;

  final MarkdownStyleSheet markdownStyle;

  const BeamThemeExtension({
    required this.borderColor,
    required this.fieldBackgroundColor,
    required this.iconColor,
    required this.primaryBackgroundTextColor,
    required this.lightGreyBackgroundTextColor,
    required this.markdownStyle,
    required this.secondaryBackgroundColor,
    required this.codeBackgroundColor,
    required this.codeRootStyle,
    required this.codeTheme,
    required this.selectedProgressColor,
    required this.unselectedProgressColor,
  });

  @override
  ThemeExtension<BeamThemeExtension> copyWith({
    Color? borderColor,
    Color? fieldBackgroundColor,
    Color? iconColor,
    Color? primaryBackgroundTextColor,
    Color? lightGreyBackgroundTextColor,
    MarkdownStyleSheet? markdownStyle,
    Color? secondaryBackgroundColor,
    Color? codeBackgroundColor,
    TextStyle? codeRootStyle,
    CodeThemeData? codeTheme,
    Color? selectedProgressColor,
    Color? unselectedProgressColor,
  }) {
    return BeamThemeExtension(
      borderColor: borderColor ?? this.borderColor,
      fieldBackgroundColor: fieldBackgroundColor ?? this.fieldBackgroundColor,
      iconColor: iconColor ?? this.iconColor,
      primaryBackgroundTextColor:
          primaryBackgroundTextColor ?? this.primaryBackgroundTextColor,
      lightGreyBackgroundTextColor:
          lightGreyBackgroundTextColor ?? this.lightGreyBackgroundTextColor,
      markdownStyle: markdownStyle ?? this.markdownStyle,
      secondaryBackgroundColor:
          secondaryBackgroundColor ?? this.secondaryBackgroundColor,
      codeBackgroundColor: codeBackgroundColor ?? this.codeBackgroundColor,
      codeRootStyle: codeRootStyle ?? this.codeRootStyle,
      codeTheme: codeTheme ?? this.codeTheme,
      selectedProgressColor:
          selectedProgressColor ?? this.selectedProgressColor,
      unselectedProgressColor:
          unselectedProgressColor ?? this.unselectedProgressColor,
    );
  }

  @override
  ThemeExtension<BeamThemeExtension> lerp(
    covariant BeamThemeExtension? other,
    double t,
  ) {
    return BeamThemeExtension(
      borderColor: Color.lerp(borderColor, other?.borderColor, t)!,
      fieldBackgroundColor:
          Color.lerp(fieldBackgroundColor, other?.fieldBackgroundColor, t)!,
      iconColor: Color.lerp(iconColor, other?.iconColor, t)!,
      primaryBackgroundTextColor: Color.lerp(
          primaryBackgroundTextColor, other?.primaryBackgroundTextColor, t)!,
      lightGreyBackgroundTextColor: Color.lerp(lightGreyBackgroundTextColor,
          other?.lightGreyBackgroundTextColor, t)!,
      markdownStyle:
          t < 0.5 ? markdownStyle : other?.markdownStyle ?? markdownStyle,
      secondaryBackgroundColor: Color.lerp(
          secondaryBackgroundColor, other?.secondaryBackgroundColor, t)!,
      codeBackgroundColor:
          Color.lerp(codeBackgroundColor, other?.codeBackgroundColor, t)!,
      codeRootStyle: TextStyle.lerp(codeRootStyle, other?.codeRootStyle, t)!,
      codeTheme: t == 0.0 ? codeTheme : other?.codeTheme ?? codeTheme,
      selectedProgressColor:
          Color.lerp(selectedProgressColor, other?.selectedProgressColor, t)!,
      unselectedProgressColor: Color.lerp(
        unselectedProgressColor,
        other?.unselectedProgressColor,
        t,
      )!,
    );
  }
}

final kLightTheme = ThemeData(
  brightness: Brightness.light,
  appBarTheme: _getAppBarTheme(BeamLightThemeColors.secondaryBackground),
  backgroundColor: BeamLightThemeColors.primaryBackground,
  canvasColor: BeamLightThemeColors.primaryBackground,
  dividerColor: BeamLightThemeColors.grey,
  elevatedButtonTheme: _getElevatedButtonTheme(BeamLightThemeColors.primary),
  outlinedButtonTheme: _getOutlineButtonTheme(
    BeamLightThemeColors.text,
    BeamLightThemeColors.primary,
  ),
  primaryColor: BeamLightThemeColors.primary,
  scaffoldBackgroundColor: BeamLightThemeColors.secondaryBackground,
  selectedRowColor: BeamLightThemeColors.selectedUnitColor,
  tabBarTheme: _getTabBarTheme(
    textColor: BeamLightThemeColors.text,
    indicatorColor: BeamLightThemeColors.primary,
  ),
  textButtonTheme: _getTextButtonTheme(BeamLightThemeColors.text),
  textTheme: _getTextTheme(BeamLightThemeColors.text),
  extensions: {
    BeamThemeExtension(
      borderColor: BeamLightThemeColors.border,
      fieldBackgroundColor: BeamLightThemeColors.grey,
      iconColor: BeamLightThemeColors.icon,
      primaryBackgroundTextColor: BeamColors.white,
      lightGreyBackgroundTextColor: BeamColors.black,
      markdownStyle: _getMarkdownStyle(Brightness.light),
      secondaryBackgroundColor: BeamLightThemeColors.secondaryBackground,
      selectedProgressColor: BeamLightThemeColors.selectedProgressColor,
      unselectedProgressColor: BeamLightThemeColors.unselectedProgressColor,
      codeBackgroundColor: BeamLightThemeColors.codeBackground,
      codeRootStyle: GoogleFonts.sourceCodePro(
        color: BeamLightThemeColors.text,
        fontSize: codeFontSize,
      ),
      codeTheme: CodeThemeData(
        styles: {
          'root': TextStyle(
            backgroundColor: BeamLightThemeColors.primaryBackground,
            color: BeamLightThemeColors.text,
          ),
          'comment': TextStyle(color: BeamLightThemeColors.codeComment),
          'quote': TextStyle(color: BeamLightThemeColors.code2),
          'variable': TextStyle(color: BeamLightThemeColors.code2),
          'keyword': TextStyle(color: BeamLightThemeColors.code2),
          'selector-tag': TextStyle(color: BeamLightThemeColors.code2),
          'built_in': TextStyle(color: BeamLightThemeColors.code2),
          'name': TextStyle(color: BeamLightThemeColors.code2),
          'tag': TextStyle(color: BeamLightThemeColors.code2),
          'string': TextStyle(color: BeamLightThemeColors.code1),
          'title': TextStyle(color: BeamLightThemeColors.code1),
          'section': TextStyle(color: BeamLightThemeColors.code1),
          'attribute': TextStyle(color: BeamLightThemeColors.code1),
          'literal': TextStyle(color: BeamLightThemeColors.code1),
          'template-tag': TextStyle(color: BeamLightThemeColors.code1),
          'template-variable': TextStyle(color: BeamLightThemeColors.code1),
          'type': TextStyle(color: BeamLightThemeColors.code1),
          'addition': TextStyle(color: BeamLightThemeColors.code1),
          'deletion': TextStyle(color: BeamLightThemeColors.code2),
          'selector-attr': TextStyle(color: BeamLightThemeColors.code2),
          'selector-pseudo': TextStyle(color: BeamLightThemeColors.code2),
          'meta': TextStyle(color: BeamLightThemeColors.code2),
          'doctag': TextStyle(color: BeamLightThemeColors.codeComment),
          'attr': TextStyle(color: BeamLightThemeColors.primary),
          'symbol': TextStyle(color: BeamLightThemeColors.code2),
          'bullet': TextStyle(color: BeamLightThemeColors.code2),
          'link': TextStyle(color: BeamLightThemeColors.code2),
          'emphasis': const TextStyle(fontStyle: FontStyle.italic),
          'strong': const TextStyle(fontWeight: FontWeight.bold),
        },
      ),
    ),
  },
);

final kDarkTheme = ThemeData(
  brightness: Brightness.dark,
  appBarTheme: _getAppBarTheme(BeamDarkThemeColors.secondaryBackground),
  backgroundColor: BeamDarkThemeColors.primaryBackground,
  canvasColor: BeamDarkThemeColors.primaryBackground,
  dividerColor: BeamDarkThemeColors.grey,
  elevatedButtonTheme: _getElevatedButtonTheme(BeamDarkThemeColors.primary),
  outlinedButtonTheme: _getOutlineButtonTheme(
    BeamDarkThemeColors.text,
    BeamDarkThemeColors.primary,
  ),
  primaryColor: BeamDarkThemeColors.primary,
  scaffoldBackgroundColor: BeamDarkThemeColors.secondaryBackground,
  selectedRowColor: BeamDarkThemeColors.selectedUnitColor,
  tabBarTheme: _getTabBarTheme(
    textColor: BeamDarkThemeColors.text,
    indicatorColor: BeamDarkThemeColors.primary,
  ),
  textButtonTheme: _getTextButtonTheme(BeamDarkThemeColors.text),
  textTheme: _getTextTheme(BeamDarkThemeColors.text),
  extensions: {
    BeamThemeExtension(
      borderColor: BeamDarkThemeColors.border,
      fieldBackgroundColor: BeamDarkThemeColors.grey,
      iconColor: BeamDarkThemeColors.icon,
      primaryBackgroundTextColor: BeamColors.white,
      lightGreyBackgroundTextColor: BeamColors.black,
      markdownStyle: _getMarkdownStyle(Brightness.dark),
      secondaryBackgroundColor: BeamDarkThemeColors.secondaryBackground,
      selectedProgressColor: BeamDarkThemeColors.selectedProgressColor,
      unselectedProgressColor: BeamDarkThemeColors.unselectedProgressColor,
      codeBackgroundColor: BeamDarkThemeColors.codeBackground,
      codeRootStyle: GoogleFonts.sourceCodePro(
        color: BeamDarkThemeColors.text,
        fontSize: codeFontSize,
      ),
      codeTheme: CodeThemeData(
        styles: {
          'root': TextStyle(
            backgroundColor: BeamDarkThemeColors.primaryBackground,
            color: BeamDarkThemeColors.text,
          ),
          'comment': TextStyle(color: BeamDarkThemeColors.codeComment),
          'quote': TextStyle(color: BeamDarkThemeColors.code2),
          'variable': TextStyle(color: BeamDarkThemeColors.code2),
          'keyword': TextStyle(color: BeamDarkThemeColors.code2),
          'selector-tag': TextStyle(color: BeamDarkThemeColors.code2),
          'built_in': TextStyle(color: BeamDarkThemeColors.code2),
          'name': TextStyle(color: BeamDarkThemeColors.code2),
          'tag': TextStyle(color: BeamDarkThemeColors.code2),
          'string': TextStyle(color: BeamDarkThemeColors.code1),
          'title': TextStyle(color: BeamDarkThemeColors.code1),
          'section': TextStyle(color: BeamDarkThemeColors.code1),
          'attribute': TextStyle(color: BeamDarkThemeColors.code1),
          'literal': TextStyle(color: BeamDarkThemeColors.code1),
          'template-tag': TextStyle(color: BeamDarkThemeColors.code1),
          'template-variable': TextStyle(color: BeamDarkThemeColors.code1),
          'type': TextStyle(color: BeamDarkThemeColors.code1),
          'addition': TextStyle(color: BeamDarkThemeColors.code1),
          'deletion': TextStyle(color: BeamDarkThemeColors.code2),
          'selector-attr': TextStyle(color: BeamDarkThemeColors.code2),
          'selector-pseudo': TextStyle(color: BeamDarkThemeColors.code2),
          'meta': TextStyle(color: BeamDarkThemeColors.code2),
          'doctag': TextStyle(color: BeamDarkThemeColors.codeComment),
          'attr': TextStyle(color: BeamDarkThemeColors.primary),
          'symbol': TextStyle(color: BeamDarkThemeColors.code2),
          'bullet': TextStyle(color: BeamDarkThemeColors.code2),
          'link': TextStyle(color: BeamDarkThemeColors.code2),
          'emphasis': const TextStyle(fontStyle: FontStyle.italic),
          'strong': const TextStyle(fontWeight: FontWeight.bold),
        },
      ),
    ),
  },
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
      headlineLarge: TextStyle(
        fontSize: 18,
        fontWeight: FontWeight.w600,
      ),
      headlineMedium: TextStyle(
        fontSize: 14,
        fontWeight: FontWeight.w600,
      ),
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
        fontSize: 14,
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
      bodySmall: TextStyle(
        fontSize: 12,
        fontWeight: FontWeight.w400,
      ),
    ).apply(
      bodyColor: textColor,
      displayColor: textColor,
    ),
  );
}

TextButtonThemeData _getTextButtonTheme(Color textColor) {
  return TextButtonThemeData(
    style: TextButton.styleFrom(
      foregroundColor: textColor,
      shape: _getButtonBorder(BeamBorderRadius.large),
    ),
  );
}

OutlinedButtonThemeData _getOutlineButtonTheme(
  Color textColor,
  Color outlineColor,
) {
  return OutlinedButtonThemeData(
    style: OutlinedButton.styleFrom(
      foregroundColor: textColor,
      side: BorderSide(color: outlineColor, width: 3),
      padding: const EdgeInsets.symmetric(
        vertical: BeamSizes.size20,
        horizontal: BeamSizes.size40,
      ),
      shape: _getButtonBorder(BeamBorderRadius.small),
    ),
  );
}

ElevatedButtonThemeData _getElevatedButtonTheme(Color color) {
  return ElevatedButtonThemeData(
    style: ElevatedButton.styleFrom(
      padding: const EdgeInsets.all(BeamSizes.size20),
      foregroundColor: BeamColors.white,
      backgroundColor: color,
    ),
  );
}

TabBarTheme _getTabBarTheme({
  required Color textColor,
  required Color indicatorColor,
}) {
  const labelStyle = TextStyle(fontWeight: FontWeight.w600);
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

AppBarTheme _getAppBarTheme(Color backgroundColor) {
  return AppBarTheme(
    color: backgroundColor,
    elevation: BeamSizes.size1,
    centerTitle: false,
    toolbarHeight: BeamSizes.appBarHeight,
  );
}

RoundedRectangleBorder _getButtonBorder(double radius) {
  return RoundedRectangleBorder(
    borderRadius: BorderRadius.all(
      Radius.circular(radius),
    ),
  );
}

MarkdownStyleSheet _getMarkdownStyle(Brightness brightness) {
  final Color primaryColor;
  final Color codeblockBackgroundColor;
  final Color textColor;
  if (brightness == Brightness.light) {
    primaryColor = BeamLightThemeColors.primary;
    codeblockBackgroundColor = BeamLightThemeColors.codeBackground;
    textColor = BeamLightThemeColors.text;
  } else {
    primaryColor = BeamDarkThemeColors.primary;
    codeblockBackgroundColor = BeamDarkThemeColors.codeBackground;
    textColor = BeamDarkThemeColors.text;
  }
  final textTheme = _getTextTheme(textColor);

  return MarkdownStyleSheet(
    p: textTheme.bodyMedium,
    pPadding: EdgeInsets.only(top: BeamSizes.size2),
    h1: textTheme.headlineLarge,
    h3: textTheme.headlineMedium,
    h3Padding: EdgeInsets.only(top: BeamSizes.size4),
    code: GoogleFonts.sourceCodePro(
      color: textColor,
      backgroundColor: BeamColors.transparent,
      fontSize: BeamSizes.size12,
    ),
    codeblockDecoration: BoxDecoration(
      color: codeblockBackgroundColor,
      border: Border.all(color: primaryColor),
      borderRadius: const BorderRadius.all(Radius.circular(BeamSizes.size4)),
    ),
  );
}

/// This is used to easily distinguish unimplemented text styles.
const TextStyle _emptyTextStyle = TextStyle();
