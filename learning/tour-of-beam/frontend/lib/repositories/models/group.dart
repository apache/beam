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

import 'node.dart';

part 'group.g.dart';

@JsonSerializable(createToJson: false)
class GroupResponseModel {
  final String title;
  final List<NodeResponseModel> nodes;

  const GroupResponseModel({
    required this.title,
    required this.nodes,
  });

  factory GroupResponseModel.fromJson(Map<String, dynamic> json) =>
      _$GroupResponseModelFromJson(json);

  // TODO(alexeyinkin): Replace with backend IDs when they start coming.
  String get id => title.toLowerCase().replaceAll(RegExp('[^a-zA-Z0-9]+'), '-');
}
