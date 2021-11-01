import 'package:flutter/material.dart';
import 'package:playground/config/theme.dart';

Map<String, TextStyle>? createTheme(ThemeColors colors) {
  return {
    'root': TextStyle(
      backgroundColor: colors.primaryBackground,
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

final kDarkCodeTheme = createTheme(ThemeColors(true));
final kLightCodeTheme = createTheme(ThemeColors(false));
