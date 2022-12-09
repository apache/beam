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
import 'package:shared_preferences/shared_preferences.dart';

const kThemeMode = 'theme_mode';

class ThemeSwitchNotifier extends ChangeNotifier {
  ThemeMode themeMode = ThemeMode.light;

  void init() {
    _setPreferences();
  }

  Future<void> _setPreferences() async {
    final preferences = await SharedPreferences.getInstance();
    themeMode = preferences.getString(kThemeMode) == ThemeMode.dark.toString()
        ? ThemeMode.dark
        : ThemeMode.light;
    notifyListeners();
  }

  bool get isDarkMode {
    return themeMode == ThemeMode.dark;
  }

  Future<void> toggleTheme() async {
    themeMode = themeMode == ThemeMode.light ? ThemeMode.dark : ThemeMode.light;
    final preferences = await SharedPreferences.getInstance();
    await preferences.setString(kThemeMode, themeMode.toString());
    notifyListeners();
  }
}

class ThemeSwitchNotifierProvider extends StatelessWidget {
  final Widget child;

  const ThemeSwitchNotifierProvider({
    super.key,
    required this.child,
  });

  @override
  Widget build(BuildContext context) {
    return ChangeNotifierProvider<ThemeSwitchNotifier>(
      create: (context) => ThemeSwitchNotifier()..init(),
      child: child,
    );
  }
}
