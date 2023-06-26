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

import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:get_it/get_it.dart';

import '../api/v1/api.pbgrpc.dart' show GetMetadataResponse;
import '../models/component_version.dart';
import '../models/sdk.dart';
import '../repositories/code_client/code_client.dart';
import '../repositories/example_client/example_client.dart';
import '../repositories/get_metadata_response_grpc_extension.dart';

/// Obtains versions from the backend.
class BuildMetadataController extends ChangeNotifier {
  ComponentVersion? _routerVersion;
  Future<GetMetadataResponse>? _routerVersionFuture;

  final _runnerVersions = <Sdk, ComponentVersion>{};
  final _runnerVersionFutures = <Sdk, Future<GetMetadataResponse>>{};

  Future<ComponentVersion> getRouterVersion() async {
    if (_routerVersionFuture == null) {
      await _loadRouterVersion();
    }
    return _routerVersion!;
  }

  Future<void> _loadRouterVersion() async {
    final client = GetIt.instance.get<ExampleClient>();
    _routerVersionFuture = client.getMetadata();

    final metadata = await _routerVersionFuture!;
    _routerVersion = metadata.componentVersion;
    notifyListeners();
  }

  /// Returns the runner version and starts loading if it is not started yet.
  Future<ComponentVersion> getRunnerVersion(Sdk sdk) async {
    if (!_runnerVersionFutures.containsKey(sdk)) {
      await _loadRunnerVersion(sdk);
    }

    return _runnerVersions[sdk]!;
  }

  Future<void> _loadRunnerVersion(Sdk sdk) async {
    final client = GetIt.instance.get<CodeClient>();
    _runnerVersionFutures[sdk] = client.getMetadata(sdk);

    final metadata = await _runnerVersionFutures[sdk]!;
    _runnerVersions[sdk] = metadata.componentVersion;
    notifyListeners();
  }
}
