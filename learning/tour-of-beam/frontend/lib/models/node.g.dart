// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'node.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

NodeModel _$NodeModelFromJson(Map<String, dynamic> json) => NodeModel(
      type: json['type'] as String,
      unit: json['unit'] == null
          ? null
          : UnitModel.fromJson(json['unit'] as Map<String, dynamic>),
      group: json['group'] == null
          ? null
          : GroupModel.fromJson(json['group'] as Map<String, dynamic>),
    );

Map<String, dynamic> _$NodeModelToJson(NodeModel instance) => <String, dynamic>{
      'type': instance.type,
      'unit': instance.unit,
      'group': instance.group,
    };
