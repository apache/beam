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

import 'package:playground/modules/messages/models/abstract_message.dart';
import 'package:playground_components/playground_components.dart';

/// A message that switches the SDK.
///
/// Sent to iframes by Beam documentation HTML when the language is switched.
class SetSdkMessage extends AbstractMessage {
  final Sdk sdk;

  static const type = 'SetSdk';

  const SetSdkMessage({
    required this.sdk,
  });

  static SetSdkMessage? tryParse(Map map) {
    if (map['type'] != type) {
      return null;
    }

    final sdk = Sdk.tryParse(map['sdk']);
    if (sdk == null) {
      return null;
    }

    return SetSdkMessage(
      sdk: sdk,
    );
  }

  @override
  int get hashCode {
    return sdk.hashCode;
  }

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) {
      return true;
    }

    return other is SetSdkMessage && sdk == other.sdk;
  }

  @override
  Map<String, dynamic> toJson() {
    return {
      'sdk': sdk.id,
    };
  }
}
