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

import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground/modules/messages/models/abstract_message.dart';
import 'package:playground_components/playground_components.dart';

/// A message that sets content for multiple snippets.
///
/// Sent by the embedded playground to a newly opened window
/// of a standalone playground.
class SetContentMessage extends AbstractMessage {
  final ExamplesLoadingDescriptor descriptor;

  static const type = 'SetContent';

  const SetContentMessage({
    required this.descriptor,
  });

  static SetContentMessage? tryParse(Map map) {
    if (map['type'] != type) {
      return null;
    }

    return SetContentMessage(
      descriptor: ExamplesLoadingDescriptorFactory.fromMap(map['descriptor']),
    );
  }

  @override
  int get hashCode {
    return descriptor.hashCode;
  }

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) {
      return true;
    }

    return other is SetContentMessage && descriptor == other.descriptor;
  }

  @override
  Map<String, dynamic> toJson() {
    return {
      'type': type,
      'descriptor': descriptor,
    };
  }
}
