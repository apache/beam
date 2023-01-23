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

import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../models/module.dart';
import '../../models/unit.dart';

abstract class TobAnalyticsService extends AnalyticsService {
  static TobAnalyticsService get() {
    return GetIt.instance.get<TobAnalyticsService>();
  }

  Future<void> openUnit(Sdk sdk, UnitModel unit);
  Future<void> closeUnit(Sdk sdk, String unitId, Duration timeSpent);
  Future<void> completeUnit(Sdk sdk, UnitModel unit);
  // TODO(nausharipov): implement
  Future<void> completeModule(Sdk sdk, ModuleModel module);
  Future<void> positiveFeedback(String feedback);
  Future<void> negativeFeedback(String feedback);
}
