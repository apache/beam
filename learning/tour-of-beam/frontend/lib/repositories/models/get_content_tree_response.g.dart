// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'get_content_tree_response.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

GetContentTreeResponse _$GetContentTreeResponseFromJson(
        Map<String, dynamic> json) =>
    GetContentTreeResponse(
      sdkId: json['sdkId'] as String,
      modules: (json['modules'] as List<dynamic>)
          .map((e) => ModuleResponseModel.fromJson(e as Map<String, dynamic>))
          .toList(),
    );
