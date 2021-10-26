// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'example_model.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

ExampleModel _$ExampleModelFromJson(Map<String, dynamic> json) => ExampleModel(
      source: json['source'] as String?,
      name: json['name'] as String?,
      type: $enumDecodeNullable(_$ExampleTypeEnumMap, json['type']),
      id: json['id'] as int?,
      description: json['description'] as String?,
    );

Map<String, dynamic> _$ExampleModelToJson(ExampleModel instance) {
  final val = <String, dynamic>{};

  void writeNotNull(String key, dynamic value) {
    if (value != null) {
      val[key] = value;
    }
  }

  writeNotNull('type', _$ExampleTypeEnumMap[instance.type]);
  writeNotNull('name', instance.name);
  writeNotNull('id', instance.id);
  writeNotNull('description', instance.description);
  writeNotNull('source', instance.source);
  return val;
}

const _$ExampleTypeEnumMap = {
  ExampleType.all: 'all',
  ExampleType.example: 'example',
  ExampleType.kata: 'kata',
  ExampleType.test: 'test',
};
