// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'example_base.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

ExampleBase _$ExampleBaseFromJson(Map<String, dynamic> json) => ExampleBase(
      name: json['name'] as String,
      path: json['path'] as String,
      sdk: Sdk.fromJson(json['sdk'] as Map<String, dynamic>),
      type: $enumDecode(_$ExampleTypeEnumMap, json['type']),
      alwaysRun: json['alwaysRun'] as bool? ?? false,
      complexity: $enumDecodeNullable(_$ComplexityEnumMap, json['complexity']),
      contextLine: json['contextLine'] as int? ?? 1,
      datasets: (json['datasets'] as List<dynamic>?)
              ?.map((e) => Dataset.fromJson(e as Map<String, dynamic>))
              .toList() ??
          const [],
      description: json['description'] as String? ?? '',
      isMultiFile: json['isMultiFile'] as bool? ?? false,
      pipelineOptions: json['pipelineOptions'] as String? ?? '',
      tags:
          (json['tags'] as List<dynamic>?)?.map((e) => e as String).toList() ??
              const [],
      urlNotebook: json['urlNotebook'] as String?,
      urlVcs: json['urlVcs'] as String?,
      viewOptions: json['viewOptions'] == null
          ? ExampleViewOptions.empty
          : ExampleViewOptions.fromJson(
              json['viewOptions'] as Map<String, dynamic>),
    );

Map<String, dynamic> _$ExampleBaseToJson(ExampleBase instance) =>
    <String, dynamic>{
      'alwaysRun': instance.alwaysRun,
      'complexity': _$ComplexityEnumMap[instance.complexity],
      'contextLine': instance.contextLine,
      'datasets': instance.datasets,
      'description': instance.description,
      'isMultiFile': instance.isMultiFile,
      'name': instance.name,
      'path': instance.path,
      'pipelineOptions': instance.pipelineOptions,
      'sdk': instance.sdk,
      'tags': instance.tags,
      'type': _$ExampleTypeEnumMap[instance.type]!,
      'urlNotebook': instance.urlNotebook,
      'urlVcs': instance.urlVcs,
      'viewOptions': instance.viewOptions,
    };

const _$ExampleTypeEnumMap = {
  ExampleType.all: 'all',
  ExampleType.example: 'example',
  ExampleType.kata: 'kata',
  ExampleType.test: 'test',
};

const _$ComplexityEnumMap = {
  Complexity.basic: 'BASIC',
  Complexity.medium: 'MEDIUM',
  Complexity.advanced: 'ADVANCED',
};
