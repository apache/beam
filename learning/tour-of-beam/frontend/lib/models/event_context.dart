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

import 'package:equatable/equatable.dart';

const _none = 'none';

/// Basic information of the Tour of Beam app state to augment analytics events.
class TobEventContext with EquatableMixin {
  const TobEventContext({
    required this.sdkId,
    required this.unitId,
  });

  final String? sdkId;
  final String? unitId;

  static const empty = TobEventContext(
    sdkId: null,
    unitId: null,
  );

  @override
  List<Object?> get props => [
        sdkId,
        unitId,
      ];

  Map<String, dynamic> toJson() => {
        'sdkId': sdkId ?? _none,
        'unitId': unitId ?? _none,
      };
}
