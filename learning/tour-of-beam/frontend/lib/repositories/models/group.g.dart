// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'group.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

GroupResponseModel _$GroupResponseModelFromJson(Map<String, dynamic> json) =>
    GroupResponseModel(
      title: json['title'] as String,
      nodes: (json['nodes'] as List<dynamic>)
          .map((e) => NodeResponseModel.fromJson(e as Map<String, dynamic>))
          .toList(),
    );
