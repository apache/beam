/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'package:json_annotation/json_annotation.dart';
import 'package:playground_components/playground_components.dart';

import 'node.dart';

part 'module.g.dart';

@JsonSerializable(createToJson: false)
class ModuleResponseModel {
  final String id;
  final String title;
  final Complexity complexity;
  final List<NodeResponseModel> nodes;

  const ModuleResponseModel({
    required this.id,
    required this.title,
    required this.complexity,
    required this.nodes,
  });

  factory ModuleResponseModel.fromJson(Map<String, dynamic> json) =>
      _$ModuleResponseModelFromJson(json);
}
