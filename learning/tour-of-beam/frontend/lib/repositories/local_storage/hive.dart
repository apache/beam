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

import 'dart:convert';

import 'package:hive/hive.dart';
import 'package:playground_components/playground_components.dart';

import 'abstract.dart';

class HiveLocalStorage extends AbstractLocalStorage {
  @override
  Future<void> saveCode(
    String unitId,
    ContentExampleLoadingDescriptor descriptor,
  ) async {
    final sdkId = descriptor.sdk.id;
    await Hive.openBox(sdkId);
    await Hive.box(sdkId).put(
      unitId,
      jsonEncode(descriptor.toJson()),
    );
  }

  @override
  Future<ContentExampleLoadingDescriptor?> getSavedCode(
    Sdk sdk,
    String unitId,
  ) async {
    final sdkId = sdk.id;
    // TODO(nausharipov) review: create a collection of boxes.
    await Hive.openBox(sdkId);
    final value = Hive.box(sdkId).get(unitId);

    if (value == null) {
      return null;
    }

    try {
      final Map<String, dynamic> map = jsonDecode(value);
      return ContentExampleLoadingDescriptor.tryParse(map);
    } on Exception catch (_) {
      rethrow;
    }
  }

  Future<Box> getBox(String sdkId) async {
    await Hive.openBox(sdkId);
    return Hive.box(sdkId);
  }
}
