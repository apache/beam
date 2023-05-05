// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'dataset.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

Dataset _$DatasetFromJson(Map<String, dynamic> json) => Dataset(
      type: $enumDecodeNullable(_$EmulatorTypeEnumMap, json['type']),
      options: Map<String, String>.from(json['options'] as Map),
      datasetPath: json['datasetPath'] as String,
    );

Map<String, dynamic> _$DatasetToJson(Dataset instance) => <String, dynamic>{
      'type': _$EmulatorTypeEnumMap[instance.type],
      'options': instance.options,
      'datasetPath': instance.datasetPath,
    };

const _$EmulatorTypeEnumMap = {
  EmulatorType.kafka: 'kafka',
};
