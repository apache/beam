// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'unit_content.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

UnitContentModel _$UnitContentModelFromJson(Map<String, dynamic> json) =>
    UnitContentModel(
      id: json['id'] as String,
      description: json['description'] as String,
      hints:
          (json['hints'] as List<dynamic>?)?.map((e) => e as String).toList() ??
              [],
      solutionSnippetId: json['solutionSnippetId'] as String?,
      title: json['title'] as String,
      taskSnippetId: json['taskSnippetId'] as String?,
    );
