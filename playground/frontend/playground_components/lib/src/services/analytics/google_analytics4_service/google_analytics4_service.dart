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

import '../analytics_service.dart';
import 'google_analytics4_service_non_web.dart'
    if (dart.library.html) 'google_analytics4_service_web.dart';

/// An umbrella class over platform implementations of Google Analytics 4.
abstract class BeamGoogleAnalytics4Service extends BeamAnalyticsService {
  factory BeamGoogleAnalytics4Service({
    required String measurementId,
  }) =>

      /// Call the platform-specific factory function.
      createGoogleAnalytics4Service(measurementId: measurementId);

  /// Since we use the default constructor as the factory,
  /// a non-factory constructor with any other name is required for subclasses.
  BeamGoogleAnalytics4Service.create();
}
