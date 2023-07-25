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
import 'package:equatable/equatable.dart';
import 'package:highlight/highlight_core.dart';
import 'package:highlight/languages/go.dart' as mode_go;
import 'package:highlight/languages/java.dart' as mode_java;
import 'package:highlight/languages/python.dart' as mode_python;
import 'package:highlight/languages/scala.dart' as mode_scala;
import 'package:json_annotation/json_annotation.dart';

part 'sdk.g.dart';

@JsonSerializable()
class Sdk with EquatableMixin {
  final String id;
  final String title;

  const Sdk({
    required this.id,
    required this.title,
  });

  factory Sdk.parseOrCreate(String id) {
    return tryParse(id) ?? Sdk(id: id, title: id);
  }

  static const java = Sdk(id: 'java', title: 'Java');
  static const go = Sdk(id: 'go', title: 'Go');
  static const python = Sdk(id: 'python', title: 'Python');
  static const scio = Sdk(id: 'scio', title: 'SCIO');

  static const known = [
    java,
    go,
    python,
    scio,
  ];

  @override
  List<Object> get props => [
        id,
        title,
      ];

  /// The default file extension of the programming language.
  String get fileExtension {
    switch (id) {
      case 'go':
        return '.go';
      case 'java':
        return '.java';
      case 'python':
        return '.py';
      case 'scio':
        return '.scala';
    }
    throw Exception('Unknown SDK: $id');
  }

  /// A temporary solution while we wait for the backend to add
  /// sdk in example responses.
  static Sdk? tryParseExamplePath(String? path) {
    if (path == null) {
      return null;
    }

    if (path.startsWith('SDK_JAVA')) {
      return java;
    }

    if (path.startsWith('SDK_GO')) {
      return go;
    }

    if (path.startsWith('SDK_PYTHON')) {
      return python;
    }

    if (path.startsWith('SDK_SCIO')) {
      return scio;
    }

    return null;
  }

  static Sdk? tryParse(Object? value) {
    if (value is! String) {
      return null;
    }

    return known.firstWhereOrNull((e) => e.id == value);
  }

  static final _idToHighlightMode = <String, Mode>{
    Sdk.java.id: mode_java.java,
    Sdk.go.id: mode_go.go,
    Sdk.python.id: mode_python.python,
    Sdk.scio.id: mode_scala.scala,
  };

  Mode? get highlightMode => _idToHighlightMode[id];

  factory Sdk.fromJson(Map<String, dynamic> json) => _$SdkFromJson(json);

  Map<String, dynamic> toJson() => _$SdkToJson(this);
}
