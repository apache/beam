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

import 'package:collection/collection.dart';
import 'package:equatable/equatable.dart';
import 'package:meta/meta.dart';

import '../sdk.dart';
import 'example_loading_descriptor.dart';

/// A factory that may parse a [map] into an [ExampleLoadingDescriptor].
///
/// [map] comes from query parameters or a serialized state of a page.
typedef SingleDescriptorFactory = ExampleLoadingDescriptor? Function(
  Map<String, dynamic> map,
);

const _descriptorsField = 'examples';
const _initialSdkField = 'sdk';
const _lazyLoadDescriptorsField = 'lazyLoad';

/// Holds information to load multiple examples.
@immutable
class ExamplesLoadingDescriptor with EquatableMixin {
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

  /// A descriptor to load nothing.
  static const empty = ExamplesLoadingDescriptor(
    descriptors: [],
  );

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

  Map<String, String> toJson() {
    final lazyLoadNormalized = lazyLoadDescriptors.map(
      (sdk, descriptors) => MapEntry(
        sdk.id,
        _descriptorsToJson(descriptors),
      ),
    );

    return {
      //
      _descriptorsField: jsonEncode(_descriptorsToJson(descriptors)),

      if (initialSdk != null) _initialSdkField: initialSdk!.id,

      if (lazyLoadNormalized.isNotEmpty)
        _lazyLoadDescriptorsField: jsonEncode(lazyLoadNormalized),
    };
  }

  List<Map> _descriptorsToJson(List<ExampleLoadingDescriptor> descriptors) {
    return descriptors.map((d) => d.toJson()).toList(growable: false);
  }

  /// Copies this and adds [addLazyLoadDescriptors] for those SDKs that
  /// are missing in [lazyLoadDescriptors].
  ExamplesLoadingDescriptor copyWithMissingLazy(
    Map<Sdk, List<ExampleLoadingDescriptor>> addLazyLoadDescriptors,
  ) {
    final newLazy = {...addLazyLoadDescriptors}..addAll(lazyLoadDescriptors);

    return ExamplesLoadingDescriptor(
      descriptors: descriptors,
      initialSdk: initialSdk,
      lazyLoadDescriptors: newLazy,
    );
  }

  ExamplesLoadingDescriptor copyWithoutViewOptions() {
    return ExamplesLoadingDescriptor(
      //
      descriptors: descriptors
          .map((d) => d.copyWithoutViewOptions())
          .toList(growable: false),

      initialSdk: initialSdk,

      lazyLoadDescriptors: lazyLoadDescriptors.map(
        (sdk, descriptors) => MapEntry(
          sdk,
          descriptors
              .map((d) => d.copyWithoutViewOptions())
              .toList(growable: false),
        ),
      ),
    );
  }

  Sdk? get initialSnippetSdk {
    if (descriptors.length == 1) {
      return descriptors.first.sdk;
    }

    for (final descriptor in descriptors) {
      if (descriptor.sdk == initialSdk) {
        return descriptor.sdk;
      }
    }

    return null;
  }

  String? get initialSnippetToken {
    if (descriptors.length == 1) {
      return descriptors.first.token;
    }

    for (final descriptor in descriptors) {
      if (descriptor.sdk == initialSdk) {
        return descriptor.token;
      }
    }

    return null;
  }

  /// Tries to parse a [map] into an [ExamplesLoadingDescriptor].
  ///
  /// [singleDescriptorFactory] is tried on nested collections of the [map].
  static ExamplesLoadingDescriptor? tryParse(
    Map<String, dynamic> map, {
    required SingleDescriptorFactory singleDescriptorFactory,
  }) {
    ExampleLoadingDescriptor? tryParseSingle(Object? map) {
      if (map is! Map<String, dynamic>) {
        return null;
      }
      return singleDescriptorFactory(map);
    }

    List<ExampleLoadingDescriptor> tryParseList(Object? list) {
      if (list is String) {
        list = jsonDecode(list); // ignore: parameter_assignments
      }

      if (list is! List) {
        return const [];
      }

      return list.map(tryParseSingle).whereNotNull().toList(growable: false);
    }

    Map<Sdk, List<ExampleLoadingDescriptor>> tryParseLazyLoad(Object? map) {
      if (map is String) {
        map = jsonDecode(map); // ignore: parameter_assignments
      }

      if (map is! Map<String, dynamic>) {
        return const {};
      }

      return map.map(
        (sdkId, descriptors) => MapEntry(
          Sdk.parseOrCreate(sdkId),
          tryParseList(descriptors),
        ),
      );
    }

    final descriptors = tryParseList(map[_descriptorsField]);
    if (descriptors.isEmpty) {
      return null;
    }

    final sdkId = map[_initialSdkField];

    return ExamplesLoadingDescriptor(
      descriptors: descriptors,
      initialSdk: sdkId == null ? null : Sdk.parseOrCreate(sdkId),
      lazyLoadDescriptors: tryParseLazyLoad(map[_lazyLoadDescriptorsField]),
    );
  }

  @override
  List<Object?> get props => [
        descriptors,
        initialSdk?.id,
        lazyLoadDescriptors,
      ];
}
