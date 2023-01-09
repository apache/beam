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

import '../api/v1/api.pbgrpc.dart' as g;
import '../models/sdk.dart';

extension SdkExtension on Sdk {
  static final _idToGrpcEnum = {
    Sdk.java.id: g.Sdk.SDK_JAVA,
    Sdk.go.id: g.Sdk.SDK_GO,
    Sdk.python.id: g.Sdk.SDK_PYTHON,
    Sdk.scio.id: g.Sdk.SDK_SCIO,
  };

  g.Sdk get grpc =>
      _idToGrpcEnum[id] ?? (throw Exception('SDK not supported for GRPS: $id'));
}

extension GrpcSdkExtension on g.Sdk {
  Sdk get model {
    switch (this) {
      case g.Sdk.SDK_JAVA:
        return Sdk.java;
      case g.Sdk.SDK_GO:
        return Sdk.go;
      case g.Sdk.SDK_PYTHON:
        return Sdk.python;
      case g.Sdk.SDK_SCIO:
        return Sdk.scio;
    }

    return Sdk(id: '$value', title: name);
  }
}
