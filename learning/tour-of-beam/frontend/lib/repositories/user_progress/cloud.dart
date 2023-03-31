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

import 'package:collection/collection.dart';
import 'package:playground_components/playground_components.dart';

import '../../enums/snippet_type.dart';
import '../client/client.dart';
import '../models/get_user_progress_response.dart';
import 'abstract.dart';

class CloudUserProgressRepository extends AbstractUserProgressRepository {
  CloudUserProgressRepository({
    required this.client,
  });
  final TobClient client;

  @override
  Future<void> completeUnit(String sdkId, String unitId) async {
    await client.postUnitComplete(sdkId, unitId);
  }

  @override
  Future<ExampleLoadingDescriptor> getSavedDescriptor({
    required Sdk sdk,
    required String unitId,
  }) async {
    final userProgressResponse = await getUserProgress(sdk);
    final unitProgress = userProgressResponse?.units.firstWhereOrNull(
      (unit) => unit.id == unitId,
    );
    final userSnippetId = unitProgress?.userSnippetId;
    if (userSnippetId == null) {
      return EmptyExampleLoadingDescriptor(sdk: sdk);
    }
    return UserSharedExampleLoadingDescriptor(
      sdk: sdk,
      snippetId: userSnippetId,
    );
  }

  @override
  Future<GetUserProgressResponse?> getUserProgress(Sdk sdk) async {
    return client.getUserProgress(sdk.id);
  }

  @override
  Future<void> saveUnitSnippet({
    required Sdk sdk,
    required List<SnippetFile> snippetFiles,
    required String unitId,
    required SnippetType? snippetType,
  }) async {
    await client.postUserCode(
      snippetFiles: snippetFiles,
      sdkId: sdk.id,
      unitId: unitId,
    );
  }

  @override
  Future<void> deleteUserProgress() async {
    await client.postDeleteUserProgress();
  }
}
