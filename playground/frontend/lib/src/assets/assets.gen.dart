/// GENERATED CODE - DO NOT MODIFY BY HAND
/// *****************************************************
///  FlutterGen
/// *****************************************************

// coverage:ignore-file
// ignore_for_file: type=lint
// ignore_for_file: directives_ordering,unnecessary_import,implicit_dynamic_list_literal

import 'package:flutter/widgets.dart';

class $AssetsTranslationsGen {
  const $AssetsTranslationsGen();

  /// File path: assets/translations/en.yaml
  String get en => 'assets/translations/en.yaml';

  /// List of all assets
  List<String> get values => [en];
}

class Assets {
  Assets._();

  static const AssetGenImage beam = AssetGenImage('assets/beam.png');
  static const AssetGenImage beamLg = AssetGenImage('assets/beam_lg.png');
  static const String colab = 'assets/colab.svg';
  static const String copy = 'assets/copy.svg';
  static const String github = 'assets/github.svg';
  static const String link = 'assets/link.svg';
  static const String multifile = 'assets/multifile.svg';
  static const String outputBottom = 'assets/output_bottom.svg';
  static const String outputLeft = 'assets/output_left.svg';
  static const String outputRight = 'assets/output_right.svg';
  static const String sendFeedback = 'assets/send_feedback.svg';
  static const String shortcuts = 'assets/shortcuts.svg';
  static const String streaming = 'assets/streaming.svg';
  static const String thumbDown = 'assets/thumb_down.svg';
  static const String thumbDownFilled = 'assets/thumb_down_filled.svg';
  static const String thumbUp = 'assets/thumb_up.svg';
  static const String thumbUpFilled = 'assets/thumb_up_filled.svg';
  static const $AssetsTranslationsGen translations = $AssetsTranslationsGen();

  /// List of all assets
  List<dynamic> get values => [
        beam,
        beamLg,
        colab,
        copy,
        github,
        link,
        multifile,
        outputBottom,
        outputLeft,
        outputRight,
        sendFeedback,
        shortcuts,
        streaming,
        thumbDown,
        thumbDownFilled,
        thumbUp,
        thumbUpFilled
      ];
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
