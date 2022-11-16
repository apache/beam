/// GENERATED CODE - DO NOT MODIFY BY HAND
/// *****************************************************
///  FlutterGen
/// *****************************************************

// coverage:ignore-file
// ignore_for_file: type=lint
// ignore_for_file: directives_ordering,unnecessary_import

import 'package:flutter/widgets.dart';

class $AssetsButtonsGen {
  const $AssetsButtonsGen();

  /// File path: assets/buttons/reset.svg
  String get reset => 'assets/buttons/reset.svg';

  /// File path: assets/buttons/theme-mode.svg
  String get themeMode => 'assets/buttons/theme-mode.svg';
}

class $AssetsNotificationIconsGen {
  const $AssetsNotificationIconsGen();

  /// File path: assets/notification_icons/error.svg
  String get error => 'assets/notification_icons/error.svg';

  /// File path: assets/notification_icons/info.svg
  String get info => 'assets/notification_icons/info.svg';

  /// File path: assets/notification_icons/success.svg
  String get success => 'assets/notification_icons/success.svg';

  /// File path: assets/notification_icons/warning.svg
  String get warning => 'assets/notification_icons/warning.svg';
}

class $AssetsPngGen {
  const $AssetsPngGen();

  /// File path: assets/png/beam-logo.png
  AssetGenImage get beamLogo => const AssetGenImage('assets/png/beam-logo.png');
}

class $AssetsSvgGen {
  const $AssetsSvgGen();

  /// File path: assets/svg/drag-horizontal.svg
  String get dragHorizontal => 'assets/svg/drag-horizontal.svg';

  /// File path: assets/svg/drag-vertical.svg
  String get dragVertical => 'assets/svg/drag-vertical.svg';
}

class $AssetsSymbolsGen {
  const $AssetsSymbolsGen();

  /// File path: assets/symbols/go.g.yaml
  String get goG => 'assets/symbols/go.g.yaml';

  /// File path: assets/symbols/python.g.yaml
  String get pythonG => 'assets/symbols/python.g.yaml';
}

class $AssetsTranslationsGen {
  const $AssetsTranslationsGen();

  /// File path: assets/translations/en.yaml
  String get en => 'assets/translations/en.yaml';
}

class Assets {
  Assets._();

  static const $AssetsButtonsGen buttons = $AssetsButtonsGen();
  static const $AssetsNotificationIconsGen notificationIcons =
      $AssetsNotificationIconsGen();
  static const $AssetsPngGen png = $AssetsPngGen();
  static const $AssetsSvgGen svg = $AssetsSvgGen();
  static const $AssetsSymbolsGen symbols = $AssetsSymbolsGen();
  static const $AssetsTranslationsGen translations = $AssetsTranslationsGen();
}

class AssetGenImage {
  const AssetGenImage(this._assetName);

  final String _assetName;

  Image image({
    Key? key,
    AssetBundle? bundle,
    ImageFrameBuilder? frameBuilder,
    ImageErrorWidgetBuilder? errorBuilder,
    String? semanticLabel,
    bool excludeFromSemantics = false,
    double? scale,
    double? width,
    double? height,
    Color? color,
    Animation<double>? opacity,
    BlendMode? colorBlendMode,
    BoxFit? fit,
    AlignmentGeometry alignment = Alignment.center,
    ImageRepeat repeat = ImageRepeat.noRepeat,
    Rect? centerSlice,
    bool matchTextDirection = false,
    bool gaplessPlayback = false,
    bool isAntiAlias = false,
    String? package,
    FilterQuality filterQuality = FilterQuality.low,
    int? cacheWidth,
    int? cacheHeight,
  }) {
    return Image.asset(
      _assetName,
      key: key,
      bundle: bundle,
      frameBuilder: frameBuilder,
      errorBuilder: errorBuilder,
      semanticLabel: semanticLabel,
      excludeFromSemantics: excludeFromSemantics,
      scale: scale,
      width: width,
      height: height,
      color: color,
      opacity: opacity,
      colorBlendMode: colorBlendMode,
      fit: fit,
      alignment: alignment,
      repeat: repeat,
      centerSlice: centerSlice,
      matchTextDirection: matchTextDirection,
      gaplessPlayback: gaplessPlayback,
      isAntiAlias: isAntiAlias,
      package: package,
      filterQuality: filterQuality,
      cacheWidth: cacheWidth,
      cacheHeight: cacheHeight,
    );
  }

  String get path => _assetName;

  String get keyName => _assetName;
}
