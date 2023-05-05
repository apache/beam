/// GENERATED CODE - DO NOT MODIFY BY HAND
/// *****************************************************
///  FlutterGen
/// *****************************************************

// coverage:ignore-file
// ignore_for_file: type=lint
// ignore_for_file: directives_ordering,unnecessary_import,implicit_dynamic_list_literal,deprecated_member_use

import 'package:flutter/widgets.dart';

class $AssetsPngGen {
  const $AssetsPngGen();

  /// File path: assets/png/laptop-dark.png
  AssetGenImage get laptopDark =>
      const AssetGenImage('assets/png/laptop-dark.png');

  /// File path: assets/png/laptop-light.png
  AssetGenImage get laptopLight =>
      const AssetGenImage('assets/png/laptop-light.png');

  /// File path: assets/png/profile-website.png
  AssetGenImage get profileWebsite =>
      const AssetGenImage('assets/png/profile-website.png');

  /// List of all assets
  List<AssetGenImage> get values => [laptopDark, laptopLight, profileWebsite];
}

class $AssetsSvgGen {
  const $AssetsSvgGen();

  /// File path: assets/svg/github-logo.svg
  String get githubLogo => 'assets/svg/github-logo.svg';

  /// File path: assets/svg/google-logo.svg
  String get googleLogo => 'assets/svg/google-logo.svg';

  /// File path: assets/svg/hint.svg
  String get hint => 'assets/svg/hint.svg';

  /// File path: assets/svg/profile-about.svg
  String get profileAbout => 'assets/svg/profile-about.svg';

  /// File path: assets/svg/profile-delete.svg
  String get profileDelete => 'assets/svg/profile-delete.svg';

  /// File path: assets/svg/profile-logout.svg
  String get profileLogout => 'assets/svg/profile-logout.svg';

  /// File path: assets/svg/solution.svg
  String get solution => 'assets/svg/solution.svg';

  /// File path: assets/svg/unit-progress-0.svg
  String get unitProgress0 => 'assets/svg/unit-progress-0.svg';

  /// File path: assets/svg/unit-progress-100.svg
  String get unitProgress100 => 'assets/svg/unit-progress-100.svg';

  /// File path: assets/svg/welcome-progress-0.svg
  String get welcomeProgress0 => 'assets/svg/welcome-progress-0.svg';

  /// List of all assets
  List<String> get values => [
        githubLogo,
        googleLogo,
        hint,
        profileAbout,
        profileDelete,
        profileLogout,
        solution,
        unitProgress0,
        unitProgress100,
        welcomeProgress0
      ];
}

class $AssetsTranslationsGen {
  const $AssetsTranslationsGen();

  /// File path: assets/translations/en.yaml
  String get en => 'assets/translations/en.yaml';

  /// List of all assets
  List<String> get values => [en];
}

class Assets {
  Assets._();

  static const $AssetsPngGen png = $AssetsPngGen();
  static const $AssetsSvgGen svg = $AssetsSvgGen();
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

  ImageProvider provider() => AssetImage(_assetName);

  String get path => _assetName;

  String get keyName => _assetName;
}
