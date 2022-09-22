// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'module.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

ModuleModel _$ModuleModelFromJson(Map<String, dynamic> json) => ModuleModel(
      moduleId: json['moduleId'] as String,
      name: json['name'] as String,
      complexity: $enumDecode(_$ComplexityEnumMap, json['complexity']),
      nodes: (json['nodes'] as List<dynamic>)
          .map((e) => NodeModel.fromJson(e as Map<String, dynamic>))
          .toList(),
    );

Map<String, dynamic> _$ModuleModelToJson(ModuleModel instance) =>
    <String, dynamic>{
      'moduleId': instance.moduleId,
      'name': instance.name,
      'complexity': _$ComplexityEnumMap[instance.complexity]!,
      'nodes': instance.nodes,
    };

const _$ComplexityEnumMap = {
  Complexity.basic: 'BASIC',
  Complexity.medium: 'MEDIUM',
  Complexity.advanced: 'ADVANCED',
};
