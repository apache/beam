// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'module.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

ModuleResponseModel _$ModuleResponseModelFromJson(Map<String, dynamic> json) =>
    ModuleResponseModel(
      id: json['id'] as String,
      title: json['title'] as String,
      complexity: $enumDecode(_$ComplexityEnumMap, json['complexity']),
      nodes: (json['nodes'] as List<dynamic>)
          .map((e) => NodeResponseModel.fromJson(e as Map<String, dynamic>))
          .toList(),
    );

const _$ComplexityEnumMap = {
  Complexity.basic: 'BASIC',
  Complexity.medium: 'MEDIUM',
  Complexity.advanced: 'ADVANCED',
};
