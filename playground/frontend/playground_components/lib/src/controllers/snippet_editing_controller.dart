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

import '../models/example.dart';
import '../models/example_loading_descriptors/content_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/empty_example_loading_descriptor.dart';
import '../models/example_loading_descriptors/example_loading_descriptor.dart';
import '../models/example_view_options.dart';
import '../models/sdk.dart';
import '../models/snippet_file.dart';
import 'snippet_file_editing_controller.dart';

/// The main state object for a single [sdk].
class SnippetEditingController extends ChangeNotifier {
  final List<SnippetFileEditingController> fileControllers = [];
  final Sdk sdk;

  Example? _selectedExample;
  ExampleLoadingDescriptor? _descriptor;
  String _pipelineOptions = '';
  bool _isChanged = false;
  SnippetFileEditingController? _activeFileController;
  final _fileControllersByName = <String, SnippetFileEditingController>{};

  SnippetEditingController({
    required this.sdk,
  });

  void setExample(
    Example example, {
    ExampleLoadingDescriptor? descriptor,
  }) {
    _descriptor = descriptor;
    _selectedExample = example;
    _pipelineOptions = example.pipelineOptions;
    _replaceFileControllers(example.files, example.viewOptions);
    _isChanged = false;

    notifyListeners();
  }

  Example? get selectedExample => _selectedExample;

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
    for (final controller in fileControllers) {
      if (controller.isChanged) {
        return true;
      }
    }

    if (_arePipelineOptionsChanged()) {
      return true;
    }

    return false;
  }

  bool _arePipelineOptionsChanged() {
    return _pipelineOptions != (_selectedExample?.pipelineOptions ?? '');
  }

  void reset() {
    for (final controller in fileControllers) {
      controller.reset();
    }

    _pipelineOptions = _selectedExample?.pipelineOptions ?? '';
  }

  /// Creates an [ExampleLoadingDescriptor] that can recover the
  /// current content.
  ExampleLoadingDescriptor getLoadingDescriptor() {
    final example = selectedExample;
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
      sdk: sdk,
    );
  }

  void _replaceFileControllers(
    Iterable<SnippetFile> files,
    ExampleViewOptions viewOptions,
  ) {
    for (final oldController in fileControllers) {
      oldController.removeListener(_onFileControllerChanged);
    }
    final newControllers = <SnippetFileEditingController>[];

    for (final file in files) {
      final controller = SnippetFileEditingController(
        contextLine1Based: file.isMain ? _selectedExample?.contextLine : null,
        savedFile: file,
        sdk: sdk,
        viewOptions: viewOptions,
      );

      newControllers.add(controller);
      controller.addListener(_onFileControllerChanged);
    }

    for (final oldController in fileControllers) {
      oldController.dispose();
    }

    fileControllers.clear();
    fileControllers.addAll(newControllers);

    _fileControllersByName.clear();
    for (final controller in newControllers) {
      _fileControllersByName[controller.savedFile.name] = controller;
    }

    _activeFileController =
        fileControllers.firstWhereOrNull((c) => c.savedFile.isMain);
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

  bool _isAnyFileControllerChanged() {
    return fileControllers.any((c) => c.isChanged);
  }

  SnippetFileEditingController? get activeFileController =>
      _activeFileController;

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
    return fileControllers.map((c) => c.getFile()).toList(growable: false);
  }
}
