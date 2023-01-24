import 'package:flutter/services.dart';

import 'native_platform.dart';

extension LogicalKeyboardKeyExtension on LogicalKeyboardKey {
  static LogicalKeyboardKey get metaOrControl => NativePlatform.isMacOs
      ? LogicalKeyboardKey.meta
      : LogicalKeyboardKey.control;
}
