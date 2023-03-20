// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'unit_progress.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

UnitProgressModel _$UnitProgressModelFromJson(Map<String, dynamic> json) =>
    UnitProgressModel(
      id: json['id'] as String,
      isCompleted: json['isCompleted'] as bool,
      userSnippetId: json['userSnippetId'] as String?,
    );

Map<String, dynamic> _$UnitProgressModelToJson(UnitProgressModel instance) =>
    <String, dynamic>{
      'id': instance.id,
      'isCompleted': instance.isCompleted,
      'userSnippetId': instance.userSnippetId,
    };
