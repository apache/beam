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

import 'package:code_text_field/code_text_field.dart';
import 'package:flutter/widgets.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/content_example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/example_loading_descriptor.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

class SnippetEditingController extends ChangeNotifier {
  final SDK sdk;
  final CodeController codeController;
  ExampleModel? _selectedExample;
  String _pipelineOptions;

  SnippetEditingController({
    required this.sdk,
    ExampleModel? selectedExample,
    String pipelineOptions = '',
  })  : codeController = CodeController(
          language: sdk.highlightMode,
          webSpaceFix: false,
        ),
        _selectedExample = selectedExample,
        _pipelineOptions = pipelineOptions;

  set selectedExample(ExampleModel? value) {
    _selectedExample = value;
    codeController.text = _selectedExample?.source ?? '';
    _pipelineOptions = _selectedExample?.pipelineOptions ?? '';
    notifyListeners();
  }

  ExampleModel? get selectedExample => _selectedExample;

  set pipelineOptions(String value) {
    _pipelineOptions = value;
    notifyListeners();
  }

  String get pipelineOptions => _pipelineOptions;

  bool get isChanged {
    return _isCodeChanged() || _arePipelineOptionsChanged();
  }

  bool _isCodeChanged() {
    return _selectedExample?.source != codeController.text;
  }

  bool _arePipelineOptionsChanged() {
    return _pipelineOptions != (_selectedExample?.pipelineOptions ?? '');
  }

  void reset() {
    codeController.text = _selectedExample?.source ?? '';
    _pipelineOptions = _selectedExample?.pipelineOptions ?? '';
  }

  /// Creates an [ExampleLoadingDescriptor] that can recover the
  /// current content.
  ExampleLoadingDescriptor getLoadingDescriptor() {
    // TODO: Return other classes for unchanged standard examples,
    //  user-shared examples, and an empty editor.
    return ContentExampleLoadingDescriptor(
      content: codeController.text,
      name: _selectedExample?.name,
      sdk: sdk,
    );
  }
}
