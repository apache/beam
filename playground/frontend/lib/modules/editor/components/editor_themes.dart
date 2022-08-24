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

import 'package:code_text_field/code_text_field.dart';
import 'package:flutter/material.dart';
import 'package:playground/config/theme.dart';

CodeThemeData createTheme(ThemeColors colors) {
  return CodeThemeData(
    styles: _createThemeStyles(colors),
  );
}

Map<String, TextStyle> _createThemeStyles(ThemeColors colors) {
  return {
    'root': TextStyle(
      backgroundColor: colors.background,
      color: colors.textColor,
    ),
    'comment': TextStyle(color: colors.codeComment),
    'quote': TextStyle(color: colors.code2),
    'variable': TextStyle(color: colors.code2),
    'keyword': TextStyle(color: colors.code2),
    'selector-tag': TextStyle(color: colors.code2),
    'built_in': TextStyle(color: colors.code2),
    'name': TextStyle(color: colors.code2),
    'tag': TextStyle(color: colors.code2),
    'string': TextStyle(color: colors.code1),
    'title': TextStyle(color: colors.code1),
    'section': TextStyle(color: colors.code1),
    'attribute': TextStyle(color: colors.code1),
    'literal': TextStyle(color: colors.code1),
    'template-tag': TextStyle(color: colors.code1),
    'template-variable': TextStyle(color: colors.code1),
    'type': TextStyle(color: colors.code1),
    'addition': TextStyle(color: colors.code1),
    'deletion': TextStyle(color: colors.code2),
    'selector-attr': TextStyle(color: colors.code2),
    'selector-pseudo': TextStyle(color: colors.code2),
    'meta': TextStyle(color: colors.code2),
    'doctag': TextStyle(color: colors.codeComment),
    'attr': TextStyle(color: colors.primary),
    'symbol': TextStyle(color: colors.code2),
    'bullet': TextStyle(color: colors.code2),
    'link': TextStyle(color: colors.code2),
    'emphasis': const TextStyle(fontStyle: FontStyle.italic),
    'strong': const TextStyle(fontWeight: FontWeight.bold),
  };
}
