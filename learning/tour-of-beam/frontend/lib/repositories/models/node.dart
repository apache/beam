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

import 'group.dart';
import 'node_type_enum.dart';
import 'unit.dart';

part 'node.g.dart';

@JsonSerializable(createToJson: false)
class NodeResponseModel {
  final NodeType type;
  final UnitResponseModel? unit;
  final GroupResponseModel? group;

  const NodeResponseModel({
    required this.type,
    required this.unit,
    required this.group,
  });

  factory NodeResponseModel.fromJson(Map<String, dynamic> json) =>
      _$NodeResponseModelFromJson(json);
}
