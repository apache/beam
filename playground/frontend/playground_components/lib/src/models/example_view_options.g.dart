// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'example_view_options.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

ExampleViewOptions _$ExampleViewOptionsFromJson(Map<String, dynamic> json) =>
    ExampleViewOptions(
      readOnlySectionNames: (json['readOnlySectionNames'] as List<dynamic>)
          .map((e) => e as String)
          .toList(),
      showSectionNames: (json['showSectionNames'] as List<dynamic>)
          .map((e) => e as String)
          .toList(),
      unfoldSectionNames: (json['unfoldSectionNames'] as List<dynamic>)
          .map((e) => e as String)
          .toList(),
      foldCommentAtLineZero: json['foldCommentAtLineZero'] as bool? ?? true,
      foldImports: json['foldImports'] as bool? ?? true,
    );

Map<String, dynamic> _$ExampleViewOptionsToJson(ExampleViewOptions instance) =>
    <String, dynamic>{
      'foldCommentAtLineZero': instance.foldCommentAtLineZero,
      'foldImports': instance.foldImports,
      'readOnlySectionNames': instance.readOnlySectionNames,
      'showSectionNames': instance.showSectionNames,
      'unfoldSectionNames': instance.unfoldSectionNames,
    };
