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
import 'package:get_it/get_it.dart';

import '../../exceptions/example_loading_exception.dart';
import '../../models/example.dart';
import '../../models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import '../../models/example_loading_descriptors/example_loading_descriptor.dart';
import '../../models/example_loading_descriptors/examples_loading_descriptor.dart';
import '../../models/sdk.dart';
import '../../services/toast_notifier.dart';
import '../playground_controller.dart';
import 'catalog_default_example_loader.dart';
import 'content_example_loader.dart';
import 'empty_example_loader.dart';
import 'example_loader.dart';
import 'example_loader_factory.dart';
import 'hive_example_loader.dart';
import 'http_example_loader.dart';
import 'standard_example_loader.dart';
import 'user_shared_example_loader.dart';

class ExamplesLoader {
  final defaultFactory = ExampleLoaderFactory();
  PlaygroundController? _playgroundController;
  ExamplesLoadingDescriptor? _descriptor;

  static final failedToLoadExamples = <String>[];

  ExamplesLoader() {
    defaultFactory.add(CatalogDefaultExampleLoader.new);
    defaultFactory.add(ContentExampleLoader.new);
    defaultFactory.add(EmptyExampleLoader.new);
    defaultFactory.add(HiveExampleLoader.new);
    defaultFactory.add(HttpExampleLoader.new);
    defaultFactory.add(StandardExampleLoader.new);
    defaultFactory.add(UserSharedExampleLoader.new);
  }

  void setPlaygroundController(PlaygroundController value) {
    _playgroundController = value;
  }

  /// Loads examples from [descriptor]'s immediate list.
  ///
  /// Sets empty editor for SDKs of failed examples.
  Future<void> loadIfNew(ExamplesLoadingDescriptor descriptor) async {
    if (_descriptor == descriptor) {
      return;
    }
    await load(descriptor);
  }

  Future<void> load(ExamplesLoadingDescriptor descriptor) async {
    _descriptor = descriptor;
    final loaders = descriptor.descriptors.map(_createLoader).whereNotNull();

    try {
      final loadFutures = loaders.map(_loadOne);
      await Future.wait(loadFutures);
    // ignore: avoid_catches_without_on_clauses
    } catch (_) {
      await _emptyMissing(loaders);
    }

    final sdk = descriptor.initialSdk;
    if (sdk != null) {
      _playgroundController!.setSdk(sdk);
    }
  }

  ExampleLoader? _createLoader(ExampleLoadingDescriptor descriptor) {
    final loader = defaultFactory.create(
      descriptor: descriptor,
      exampleCache: _playgroundController!.exampleCache,
    );

    if (loader == null) {
      // TODO(alexeyinkin): Log, https://github.com/apache/beam/issues/23398.
      print('Cannot create example loader for $descriptor');
      return null;
    }

    return loader;
  }

  Future<void> _emptyMissing(Iterable<ExampleLoader> loaders) async {
    await Future.wait(loaders.map(_emptyIfMissing));
  }

  Future<void> _emptyIfMissing(ExampleLoader loader) async {
    final sdk = loader.sdk;

    if (sdk == null) {
      return;
    }

    _playgroundController!.setEmptyIfNotExists(
      sdk,
      setCurrentSdk: _shouldSetCurrentSdk(sdk),
    );
  }

  Future<void> loadDefaultIfAny(Sdk sdk) async {
    try {
      final one = _descriptor?.lazyLoadDescriptors[sdk]?.firstOrNull;

      if (_descriptor == null || one == null) {
        return;
      }

      final loader = _createLoader(one);
      if (loader == null) {
        return;
      }

      await _loadOne(loader);
    } on Exception catch (ex) {
      GetIt.instance.get<ToastNotifier>().addException(ex);
      await _loadOne(
        EmptyExampleLoader(
          descriptor: EmptyExampleLoadingDescriptor(sdk: sdk),
          exampleCache: _playgroundController!.exampleCache,
        ),
      );
      rethrow;
    }
  }

  Future<void> _loadOne(ExampleLoader loader) async {
    Example example;
    try {
      example = await loader.future;
    // ignore: avoid_catches_without_on_clauses
    } catch (ex) {
      example = Example.empty(loader.sdk ?? Sdk.java);
      _handleLoadException(loader, ex as Exception);
      throw ExampleLoadingException(token: loader.descriptor.token);
    }
    _playgroundController!.setExample(
      example,
      descriptor: loader.descriptor,
      setCurrentSdk: _shouldSetCurrentSdk(example.sdk),
    );
  }

  void _handleLoadException(ExampleLoader loader, Exception ex) {
    if (loader.descriptor.token != null) {
      failedToLoadExamples.add(loader.descriptor.token!);
    }
    GetIt.instance.get<ToastNotifier>().addException(ex);
    final example = Example.empty(loader.sdk ?? Sdk.java);
    _playgroundController!.setExample(
      example,
      descriptor: loader.descriptor,
      setCurrentSdk: _shouldSetCurrentSdk(example.sdk),
    );
  }

  bool _shouldSetCurrentSdk(Sdk sdk) {
    final descriptor = _descriptor;

    if (descriptor == null) {
      return false;
    }

    if (descriptor.initialSdk == null) {
      return true;
    }

    return descriptor.initialSdk == sdk;
  }
}
