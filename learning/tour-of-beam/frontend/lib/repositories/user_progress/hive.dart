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

import '../../constants/hive_box_names.dart';
import '../../models/unit_progress.dart';
import '../models/get_user_progress_response.dart';
import 'abstract.dart';

class HiveUserProgressRepository extends AbstractUserProgressRepository {
  @override
  Future<void> completeUnit(String sdkId, String unitId) {
    throw UnimplementedError();
  }

  @override
  Future<ExampleLoadingDescriptor> getSavedSnippet({
    required Sdk sdk,
    required String unitId,
  }) async {
    try {
      final unitProgressEncoded = Hive.box(
        // TODO(nausharipov) review: use hive collections or something better?
        _getSdkBoxName(sdk, HiveBoxNames.unitProgress),
      ).get(unitId);
      final unitProgress =
          UnitProgressModel.fromJson(jsonDecode(unitProgressEncoded));
      return HiveExampleLoadingDescriptor(
        boxName: _getSdkBoxName(sdk, HiveBoxNames.snippets),
        sdk: sdk,
        snippetId: unitProgress.cachedSnippetId!,
      );
    } on Exception catch (_) {
      return EmptyExampleLoadingDescriptor(sdk: sdk);
    }
  }

  @override
  Future<GetUserProgressResponse?> getUserProgress(String sdkId) async {
    final sdkUnitProgressBox =
        await Hive.openBox(sdkId + HiveBoxNames.unitProgress);
    return GetUserProgressResponse.fromJson({
      // ignore: unnecessary_lambdas
      'units': sdkUnitProgressBox.values.map((e) => jsonDecode(e)).toList(),
    });
  }

  @override
  Future<void> saveUnitSnippet({
    required Sdk sdk,
    required List<SnippetFile> snippetFiles,
    required String unitId,
  }) async {
    final box = await Hive.openBox(
      _getSdkBoxName(sdk, HiveBoxNames.snippets),
    );
    await _saveUnitProgress(sdk: sdk, unitId: unitId);
    await box.put(
      unitId,
      jsonEncode(
        Example(
          files: snippetFiles,
          name: 'name',
          sdk: sdk,
          type: ExampleType.example,
          path: 'path',
        ).toJson(),
      ),
    );
  }

  Future<void> _saveUnitProgress({
    required Sdk sdk,
    required String unitId,
  }) async {
    final box = await Hive.openBox(
      _getSdkBoxName(sdk, HiveBoxNames.unitProgress),
    );
    final unitProgressEncoded = box.get(unitId);
    if (unitProgressEncoded == null) {
      await box.put(
        unitId,
        jsonEncode(
          UnitProgressModel(
            id: unitId,
            // TODO(nausharipov) review: avoid hardcode?
            isCompleted: false,
            userSnippetId: null,
            cachedSnippetId: unitId,
          ).toJson(),
        ),
      );
    }
  }

  String _getSdkBoxName(Sdk sdk, String boxName) {
    return sdk.id + boxName;
  }
}
