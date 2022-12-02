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
import 'package:meta/meta.dart';

import '../sdk.dart';
import 'example_loading_descriptor.dart';

@immutable
class ExamplesLoadingDescriptor {
  /// The descriptors to be loaded right away.
  final List<ExampleLoadingDescriptor> descriptors;

  /// The descriptors to be loaded when an SDK is selected
  /// that has nothing loaded yet.
  final Map<Sdk, List<ExampleLoadingDescriptor>> lazyLoadDescriptors;

  /// If set, sets the SDK to this and does not change it when loading
  /// new examples. Otherwise sets the SDK to that of each loaded example
  /// of [descriptors].
  final Sdk? initialSdk;

  const ExamplesLoadingDescriptor({
    required this.descriptors,
    this.lazyLoadDescriptors = const {},
    this.initialSdk,
  });

  @override
  String toString() {
    final buffer = StringBuffer();
    buffer.write('Descriptors: ');
    buffer.write(descriptors.map((e) => e.toString()).join('_'));

    for (final descriptor in lazyLoadDescriptors.entries) {
      buffer.write(', Lazy Load ${descriptor.key.id}: ');
      buffer.write(descriptor.value.map((e) => e.toString()).join('_'));
    }

    return buffer.toString();
  }

  @override
  int get hashCode => Object.hash(
        const ListEquality().hash(descriptors),
        const DeepCollectionEquality().hash(lazyLoadDescriptors),
      );

  @override
  bool operator ==(Object other) {
    return other is ExamplesLoadingDescriptor &&
        const ListEquality().equals(descriptors, other.descriptors) &&
        const DeepCollectionEquality().equals(
          lazyLoadDescriptors,
          other.lazyLoadDescriptors,
        );
  }

  Map<String, dynamic> toJson() {
    return {
      'descriptors': descriptors.map((d) => d.toJson()).toList(growable: false),
    };
  }
}
