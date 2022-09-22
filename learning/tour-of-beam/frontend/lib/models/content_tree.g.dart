// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'content_tree.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

ContentTreeModel _$ContentTreeModelFromJson(Map<String, dynamic> json) =>
    ContentTreeModel(
      sdk: json['sdk'] as String,
      modules: (json['modules'] as List<dynamic>)
          .map((e) => ModuleModel.fromJson(e as Map<String, dynamic>))
          .toList(),
    );

Map<String, dynamic> _$ContentTreeModelToJson(ContentTreeModel instance) =>
    <String, dynamic>{
      'sdk': instance.sdk,
      'modules': instance.modules,
    };
