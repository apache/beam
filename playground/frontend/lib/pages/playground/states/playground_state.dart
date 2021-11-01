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

import 'dart:math';

import 'package:flutter/material.dart';
import 'package:playground/modules/editor/repository/code_repository/code_repository.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_request.dart';
import 'package:playground/modules/editor/repository/code_repository/run_code_result.dart';
import 'package:playground/modules/examples/models/example_model.dart';
import 'package:playground/modules/sdk/models/sdk.dart';

const kTitleLength = 15;
const kTitle = 'Catalog';

class PlaygroundState with ChangeNotifier {
  late SDK _sdk;
  CodeRepository? _codeRepository;
  ExampleModel? _selectedExample;
  String _source = '';
  RunCodeResult? _result;

  String get examplesTitle {
    final name = _selectedExample?.name ?? '';
    return name.substring(0, min(kTitleLength, name.length));
  }

  PlaygroundState({
    SDK sdk = SDK.java,
    ExampleModel? selectedExample,
    CodeRepository? codeRepository,
  }) {
    _selectedExample = selectedExample;
    _sdk = sdk;
    _source = _selectedExample?.sources[_sdk] ?? '';
    _codeRepository = codeRepository;
  }

  ExampleModel? get selectedExample => _selectedExample;

  SDK get sdk => _sdk;

  String get source => _source;

  bool get isCodeRunning => result?.status == RunCodeStatus.executing;

  RunCodeResult? get result => _result;

  setExample(ExampleModel example) {
    _selectedExample = example;
    _source = example.sources[_sdk] ?? '';
    notifyListeners();
  }

  setSdk(SDK sdk) {
    _sdk = sdk;
    notifyListeners();
  }

  setSource(String source) {
    _source = source;
  }

  clearOutput() {
    _result = null;
    notifyListeners();
  }

  reset() {
    _sdk = SDK.java;
    _source = _selectedExample?.sources[_sdk] ?? '';
    notifyListeners();
  }

  resetError() {
    if (result == null) {
      return;
    }
    _result = RunCodeResult(status: result!.status, output: result!.output);
    notifyListeners();
  }

  void runCode() {
    _codeRepository
        ?.runCode(RunCodeRequestWrapper(code: source, sdk: sdk))
        .listen((event) {
      _result = event;
      notifyListeners();
    });
  }
}
