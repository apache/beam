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

import 'package:flutter/widgets.dart';
import 'package:get_it/get_it.dart';
import 'package:playground_components/playground_components.dart';

import '../../../cache/unit_progress.dart';
import '../../../models/event_context.dart';
import '../../../models/unit.dart';
import '../../../repositories/client/client.dart';
import '../../../services/analytics/events/unit_completed.dart';

/// The state object for the [unit] being currently open.
class UnitController extends ChangeNotifier {
  final UnitModel unit;
  final Sdk sdk;

  UnitController({
    required this.unit,
    required this.sdk,
  });

  Future<void> completeUnit() async {
    final client = GetIt.instance.get<TobClient>();
    final unitProgressCache = GetIt.instance.get<UnitProgressCache>();
    try {
      unitProgressCache.addUpdatingUnitId(unit.id);
      await client.postUnitComplete(sdk.id, unit.id);
    } finally {
      PlaygroundComponents.analyticsService.sendUnawaited(
        UnitCompletedTobAnalyticsEvent(
          tobContext: TobEventContext(
            sdkId: sdk.id,
            unitId: unit.id,
          ),
        ),
      );
      await unitProgressCache.updateCompletedUnits();
      unitProgressCache.clearUpdatingUnitId(unit.id);
    }
  }
}
