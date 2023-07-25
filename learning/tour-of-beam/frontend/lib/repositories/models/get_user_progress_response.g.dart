// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'get_user_progress_response.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

GetUserProgressResponse _$GetUserProgressResponseFromJson(
        Map<String, dynamic> json) =>
    GetUserProgressResponse(
      units: (json['units'] as List<dynamic>)
          .map((e) => UnitProgressModel.fromJson(e as Map<String, dynamic>))
          .toList(),
    );
