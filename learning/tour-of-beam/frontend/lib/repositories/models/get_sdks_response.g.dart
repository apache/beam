// GENERATED CODE - DO NOT MODIFY BY HAND

part of 'get_sdks_response.dart';

// **************************************************************************
// JsonSerializableGenerator
// **************************************************************************

GetSdksResponse _$GetSdksResponseFromJson(Map<String, dynamic> json) =>
    GetSdksResponse(
      sdks: (json['sdks'] as List<dynamic>)
          .map((e) => Sdk.fromJson(e as Map<String, dynamic>))
          .toList(),
    );
