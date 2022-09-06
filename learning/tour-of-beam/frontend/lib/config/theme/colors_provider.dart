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
import 'package:provider/provider.dart';

import '../../constants/colors.dart';

class ThemeColorsProvider extends StatelessWidget {
  final ThemeColors data;
  final Widget child;

  const ThemeColorsProvider({
    super.key,
    required this.data,
    required this.child,
  });

  @override
  Widget build(BuildContext context) {
    return Provider<ThemeColors>.value(
      value: data,
      child: child,
    );
  }
}

class ThemeColors {
  final Color? _background;
  final bool isDark;

  ThemeColors({
    required this.isDark,
    Color? background,
  }) : _background = background;

  static ThemeColors of(BuildContext context, {bool listen = true}) {
    return Provider.of<ThemeColors>(context, listen: listen);
  }

  const ThemeColors.fromBrightness({
    required this.isDark,
  }) : _background = null;

  Color get divider =>
      isDark ? TobDarkThemeColors.grey : TobLightThemeColors.grey;

  Color get primary =>
      isDark ? TobDarkThemeColors.primary : TobLightThemeColors.primary;

  Color get primaryBackgroundTextColor => TobColors.white;

  Color get lightGreyBackgroundTextColor => TobColors.black;

  Color get secondaryBackground => isDark
      ? TobDarkThemeColors.secondaryBackground
      : TobLightThemeColors.secondaryBackground;

  Color get background =>
      _background ??
      (isDark
          ? TobDarkThemeColors.primaryBackground
          : TobLightThemeColors.primaryBackground);

  Color get textColor =>
      isDark ? TobDarkThemeColors.text : TobLightThemeColors.text;

  Color get progressBackgroundColor =>
      // TODO(nausharipov): reuse these colors after discussion with Anna
      isDark ? const Color(0xffFFFFFF) : const Color(0xff242639);
}
