// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'node.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

NodeResponseModel _$NodeResponseModelFromJson(Map<String, dynamic> json) =>
    NodeResponseModel(
      type: $enumDecode(_$NodeTypeEnumMap, json['type']),
      unit: json['unit'] == null
          ? null
          : UnitResponseModel.fromJson(json['unit'] as Map<String, dynamic>),
      group: json['group'] == null
          ? null
          : GroupResponseModel.fromJson(json['group'] as Map<String, dynamic>),
    );

const _$NodeTypeEnumMap = {
  NodeType.group: 'group',
  NodeType.unit: 'unit',
};
