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

import 'package:grpc/grpc_connection_interface.dart';

// ignore: implementation_imports
import 'package:grpc/src/client/transport/xhr_transport.dart';

class IisWorkaroundChannel extends ClientChannelBase {
  final Uri uri;

  IisWorkaroundChannel.xhr(this.uri) : super();

  @override
  ClientConnection createConnection() {
    return _IisClientConnection(uri);
  }
}

class _IisClientConnection extends XhrClientConnection {
  _IisClientConnection(super.uri);

  @override
  GrpcTransportStream makeRequest(
    String path,
    Duration? timeout,
    Map<String, String> metadata,
    ErrorHandler onError, {
    CallOptions? callOptions,
  }) {
    final pathWithoutFirstSlash = path.substring(1);

    return super.makeRequest(
      pathWithoutFirstSlash,
      timeout,
      metadata,
      onError,
      callOptions: callOptions,
    );
  }
}
