import 'package:json_annotation/json_annotation.dart';

@JsonSerializable()
class OutputsModel {
  final String output;
  final String graph;
  final String log;

  OutputsModel(this.output, this.graph, this.log);
}