// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'snippet_file.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

SnippetFile _$SnippetFileFromJson(Map<String, dynamic> json) => SnippetFile(
      content: json['content'] as String,
      isMain: json['isMain'] as bool,
      name: json['name'] as String? ?? '',
    );

Map<String, dynamic> _$SnippetFileToJson(SnippetFile instance) =>
    <String, dynamic>{
      'content': instance.content,
      'isMain': instance.isMain,
      'name': instance.name,
    };
