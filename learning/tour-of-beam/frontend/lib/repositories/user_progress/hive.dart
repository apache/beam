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

import 'package:get_it/get_it.dart';
import 'package:hive/hive.dart';
import 'package:playground_components/playground_components.dart';

import '../../cache/sdk.dart';
import '../../constants/hive_box_names.dart';
import '../../enums/snippet_type.dart';
import '../../models/unit_progress.dart';
import '../models/get_user_progress_response.dart';
import 'abstract.dart';

class HiveUserProgressRepository extends AbstractUserProgressRepository {
  @override
  Future<void> completeUnit(String sdkId, String unitId) {
    throw UnimplementedError();
  }

  @override
  Future<ExampleLoadingDescriptor> getSavedDescriptor({
    required Sdk sdk,
    required String unitId,
  }) async {
    try {
      final unitProgressBox = await Hive.openBox(
        HiveBoxNames.getSdkBoxName(sdk, HiveBoxNames.unitProgress),
      );
      final unitProgress = UnitProgressModel.fromJson(
        jsonDecode(unitProgressBox.get(unitId)),
      );
      return HiveExampleLoadingDescriptor(
        boxName: HiveBoxNames.getSdkBoxName(sdk, HiveBoxNames.snippets),
        sdk: sdk,
        snippetId: unitProgress.userSnippetId!,
      );
    } on Exception {
      return EmptyExampleLoadingDescriptor(sdk: sdk);
    }
  }

  @override
  Future<GetUserProgressResponse?> getUserProgress(Sdk sdk) async {
    final sdkUnitProgressBox = await Hive.openBox(
      HiveBoxNames.getSdkBoxName(sdk, HiveBoxNames.unitProgress),
    );
    return GetUserProgressResponse.fromJson({
      // TODO(nausharipov): Replace lambda with tear-off when this lands: https://github.com/dart-lang/language/issues/1813
      'units': sdkUnitProgressBox.values.map((e) => jsonDecode(e)).toList(),
    });
  }

  @override
  Future<void> saveUnitSnippet({
    required Sdk sdk,
    required List<SnippetFile> snippetFiles,
    required SnippetType snippetType,
    required String unitId,
  }) async {
    final snippetsBox = await Hive.openBox(
      HiveBoxNames.getSdkBoxName(sdk, HiveBoxNames.snippets),
    );
    final snippetId = 'local_$unitId';

    await _saveUnitProgressIfUnsaved(
      sdk: sdk,
      unitId: unitId,
      userSnippetId: snippetId,
    );

    await snippetsBox.put(
      snippetId,
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

  Future<void> _saveUnitProgressIfUnsaved({
    required Sdk sdk,
    required String unitId,
    required String userSnippetId,
  }) async {
    final unitProgressBox = await Hive.openBox(
      HiveBoxNames.getSdkBoxName(sdk, HiveBoxNames.unitProgress),
    );
    final unitProgressEncoded = unitProgressBox.get(unitId);
    if (unitProgressEncoded == null) {
      await unitProgressBox.put(
        unitId,
        jsonEncode(
          UnitProgressModel(
            id: unitId,
            isCompleted: false,
            userSnippetId: userSnippetId,
          ).toJson(),
        ),
      );
    }
  }

  @override
  Future<void> deleteUserProgress() async {
    final sdks = GetIt.instance.get<SdkCache>().getSdks();
    for (final sdk in sdks) {
      final unitProgress = await Hive.openBox(
        HiveBoxNames.getSdkBoxName(sdk, HiveBoxNames.unitProgress),
      );
      final snippetsBox = await Hive.openBox(
        HiveBoxNames.getSdkBoxName(sdk, HiveBoxNames.snippets),
      );
      await unitProgress.clear();
      await snippetsBox.clear();
    }
  }
}
