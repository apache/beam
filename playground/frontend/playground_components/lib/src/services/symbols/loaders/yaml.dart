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

import 'package:flutter/services.dart';
import 'package:yaml/yaml.dart';

import '../../../models/symbols_dictionary.dart';
import 'abstract.dart';

class YamlSymbolsLoader extends AbstractSymbolsLoader {
  final String path;
  final String? package;

  YamlSymbolsLoader({
    required this.path,
    this.package,
  });

  static const _methodsKey = 'methods';

  @override
  late Future<SymbolsDictionary> future = _load();

  Future<SymbolsDictionary> _load() async {
    final map = await _getMap();
    final list = <String>[];

    for (final classEntry in map.entries) {
      list.add(classEntry.key);

      final classValue = classEntry.value;

      if (classValue is! Map) {
        throw Exception('Expected map for ${classEntry.key}, got $classValue');
      }

      final methods = classValue[_methodsKey] as List?;
      list.addAll(methods?.cast<String>() ?? []);
    }

    return SymbolsDictionary(symbols: list);
  }

  Future<Map> _getMap() async {
    final fullPath = package == null ? path : 'packages/$package/$path';
    final yaml = loadYaml(await rootBundle.loadString(fullPath));

    if (yaml is! Map) {
      throw Exception('Expecting a YAML map, got $yaml');
    }

    return yaml;
  }
}
