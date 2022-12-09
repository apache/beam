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

import 'package:easy_localization_ext/easy_localization_ext.dart';

import 'locator.dart';

class PlaygroundComponents {
  static const packageName = 'playground_components';

  // TODO(alexeyinkin): Make const when this is fixed: https://github.com/aissat/easy_localization_loader/issues/39
  static final translationLoader = YamlPackageAssetLoader(
    packageName,
    path: 'assets/translations',
  );

  static Future<void> ensureInitialized() async {
    await initializeServiceLocator();
  }
}
