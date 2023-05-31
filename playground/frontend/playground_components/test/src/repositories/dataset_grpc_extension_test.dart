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

import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/src/api/v1/api.pbgrpc.dart' as g;
import 'package:playground_components/src/repositories/dataset_grpc_extension.dart';

void main() {
  final datasets = <g.Dataset>[
    //
    g.Dataset(
      datasetPath: 'mockPath1',
      options: {'key1': 'value1'},
      type: g.EmulatorType.EMULATOR_TYPE_KAFKA,
    ),

    g.Dataset(
      datasetPath: 'mockPath2',
      options: {'key2': 'value2'},
      type: g.EmulatorType.EMULATOR_TYPE_UNSPECIFIED,
    ),
  ];

  group('Dataset extensions test.', () {
    for (final dataset in datasets) {
      test('Dataset with type ${dataset.type.name} converts to the same value',
          () {
        expect(dataset.model.grpc, dataset);
      });
    }
  });
}
