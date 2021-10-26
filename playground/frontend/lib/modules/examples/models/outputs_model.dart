import 'package:json_annotation/json_annotation.dart';

part 'outputs_model.g.dart';

@JsonSerializable()
class OutputsModel {
  final String output;
  final String graph;
  final String log;

  OutputsModel(this.output, this.graph, this.log);

  factory OutputsModel.fromJson(Map<String, dynamic> data) =>
      _$OutputsModelFromJson(data);

  Map<String, dynamic> toJson() => _$OutputsModelToJson(this);
}
