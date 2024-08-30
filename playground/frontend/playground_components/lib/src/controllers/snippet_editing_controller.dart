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
import 'package:flutter/widgets.dart';

import '../models/event_snippet_context.dart';
import '../models/example.dart';
import '../models/example_loading_descriptors/content_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/example_loading_descriptor.dart';
import '../models/example_view_options.dart';
import '../models/loading_status.dart';
import '../models/sdk.dart';
import '../models/snippet_file.dart';
import 'snippet_file_editing_controller.dart';

/// The main state object for a single [sdk].
class SnippetEditingController extends ChangeNotifier {
  final Sdk sdk;

  ExampleLoadingDescriptor? _descriptor;
  Example? _example;
  String _pipelineOptions = '';

  bool _isChanged = false;
  LoadingStatus _exampleLoadingStatus = LoadingStatus.done;

  SnippetFileEditingController? _activeFileController;
  final _fileControllers = <SnippetFileEditingController>[];
  final _fileControllersByName = <String, SnippetFileEditingController>{};

  Map<String, dynamic> _defaultEventParams = const {};

  void setDefaultEventParams(Map<String, dynamic> eventParams) {
    _defaultEventParams = eventParams;
    for (final fileController in _fileControllers) {
      fileController.defaultEventParams = eventParams;
    }
  }

  SnippetEditingController({
    required this.sdk,
  });

  /// Attempts to acquire a lock for asynchronous example loading.
  ///
  /// This prevents race condition for quick example switching
  /// and allows to show a loading indicator.
  ///
  /// Returns whether the lock was acquired.
  bool lockExampleLoading() {
    switch (_exampleLoadingStatus) {
      case LoadingStatus.loading:
        return false;
      case LoadingStatus.done:
      case LoadingStatus.error:
        _exampleLoadingStatus = LoadingStatus.loading;
        return true;
    }
  }

  void releaseExampleLoading() {
    _exampleLoadingStatus = LoadingStatus.done;
  }

  bool get isLoading => _exampleLoadingStatus == LoadingStatus.loading;

  void setExample(
    Example example, {
    ExampleLoadingDescriptor? descriptor,
  }) {
    _descriptor = descriptor;
    _example = example;
    _pipelineOptions = example.pipelineOptions;
    _isChanged = false;
    releaseExampleLoading();

    _deleteFileControllers();
    _createFileControllers(example.files, example.viewOptions);

    notifyListeners();
  }

  Example? get example => _example;

  ExampleLoadingDescriptor? get descriptor => _descriptor;

  set pipelineOptions(String value) {
    if (value == _pipelineOptions) {
      return;
    }
    _pipelineOptions = value;

    if (!_isChanged) {
      if (_arePipelineOptionsChanged()) {
        _isChanged = true;
        notifyListeners();
      }
    } else {
      _updateIsChanged();
      if (!_isChanged) {
        notifyListeners();
      }
    }
  }

  String get pipelineOptions => _pipelineOptions;

  bool get isChanged => _isChanged;

  void _updateIsChanged() {
    _isChanged = _calculateIsChanged();
  }

  bool _calculateIsChanged() {
    return _isAnyFileControllerChanged() || _arePipelineOptionsChanged();
  }

  bool _isAnyFileControllerChanged() {
    return _fileControllers.any((c) => c.isChanged);
  }

  bool _arePipelineOptionsChanged() {
    return _pipelineOptions != (_example?.pipelineOptions ?? '');
  }

  void reset() {
    for (final controller in _fileControllers) {
      controller.reset();
    }

    _pipelineOptions = _example?.pipelineOptions ?? '';
  }

  /// Creates an [ExampleLoadingDescriptor] that can recover the
  /// current content.
  ExampleLoadingDescriptor getLoadingDescriptor() {
    final example = this.example;
    if (example == null) {
      return EmptyExampleLoadingDescriptor(sdk: sdk);
    }

    if (!isChanged && _descriptor != null) {
      return _descriptor!;
    }

    return ContentExampleLoadingDescriptor(
      complexity: example.complexity,
      files: getFiles(),
      name: example.name,
      pipelineOptions: _pipelineOptions,
      sdk: sdk,
    );
  }

  void _deleteFileControllers() {
    for (final controller in _fileControllers) {
      controller.removeListener(_onFileControllerChanged);
      controller.dispose();
    }

    _fileControllers.clear();
    _fileControllersByName.clear();
  }

  void _createFileControllers(
    Iterable<SnippetFile> files,
    ExampleViewOptions viewOptions,
  ) {
    for (final file in files) {
      final controller = SnippetFileEditingController(
        contextLine1Based: file.isMain ? _example?.contextLine : null,
        savedFile: file,
        sdk: sdk,
        viewOptions: viewOptions,
      );

      _fileControllers.add(controller);
      controller.addListener(_onFileControllerChanged);
    }

    for (final controller in _fileControllers) {
      _fileControllersByName[controller.savedFile.name] = controller;
    }

    _activeFileController =
        _fileControllers.firstWhereOrNull((c) => c.savedFile.isMain);

    setDefaultEventParams(_defaultEventParams);
  }

  void _onFileControllerChanged() {
    if (!_isChanged) {
      if (_isAnyFileControllerChanged()) {
        _isChanged = true;
        notifyListeners();
      }
    } else {
      _updateIsChanged();
      if (!_isChanged) {
        notifyListeners();
      }
    }
  }

  List<SnippetFileEditingController> get fileControllers =>
      UnmodifiableListView(_fileControllers);

  SnippetFileEditingController? get activeFileController =>
      _activeFileController;

  SnippetFileEditingController requireFileControllerByName(String name) {
    final result = getFileControllerByName(name);

    if (result != null) {
      return result;
    }

    throw Exception(
      'Required SnippetFileEditingController for $name, '
      'only have ${_fileControllers.map((c) => c.getFile().name)}, '
      '${example?.path} ${example?.name}',
    );
  }

  SnippetFileEditingController? getFileControllerByName(String name) {
    return _fileControllersByName[name];
  }

  void activateFileControllerByName(String name) {
    final newController = getFileControllerByName(name);

    if (newController != _activeFileController) {
      _activeFileController = newController;
      notifyListeners();
    }
  }

  List<SnippetFile> getFiles() {
    return _fileControllers.map((c) => c.getFile()).toList(growable: false);
  }

  EventSnippetContext get eventSnippetContext {
    final descriptor = getLoadingDescriptor();

    return EventSnippetContext(
      originalSnippet: _descriptor?.token,
      sdk: sdk,
      snippet: descriptor.token,
    );
  }

  bool shouldSaveBeforeSharing() {
    if (!(descriptor?.isSerializableToUrl ?? false)) {
      return true;
    }

    if (isChanged) {
      return true;
    }

    return false;
  }
}
