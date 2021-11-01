import 'package:flutter/material.dart';
import 'package:google_fonts/google_fonts.dart';

TextStyle getTitleFontStyle({TextStyle? textStyle}) {
  return GoogleFonts.roboto(textStyle: textStyle);
}

TextStyle getCodeFontStyle({TextStyle? textStyle}) {
  return GoogleFonts.sourceCodePro(textStyle: textStyle);
}

TextTheme getBaseFontTheme(TextTheme theme) {
  return GoogleFonts.sourceSansProTextTheme(theme);
}
