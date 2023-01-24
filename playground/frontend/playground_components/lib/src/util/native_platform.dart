import 'package:os_detect/os_detect.dart' as platform;

class NativePlatform {
  static bool get isMacOs =>
      platform.operatingSystemVersion.contains('Mac OS') ||
      platform.operatingSystemVersion.contains('Macintosh');
}
