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

import 'package:flutter_test/flutter_test.dart';
import 'package:highlight/highlight_core.dart';
import 'package:playground_components/src/models/symbols_dictionary.dart';
import 'package:playground_components/src/services/symbols/loaders/abstract.dart';
import 'package:playground_components/src/services/symbols/symbols_notifier.dart';

void main() {
  test(
    'SymbolsNotifier loads symbols from a loader and notifies listeners',
    () async {
      const symbols = ['a', 'b', 'c'];
      final mode = Mode();
      final notifier = SymbolsNotifier();
      int notified = 0;
      List<String>? loadedSymbols;

      notifier.addListener(() {
        notified++;
        loadedSymbols =
            List<String>.from(notifier.getDictionary(mode)!.symbols);
      });
      notifier.addLoaderIfNot(mode, const _TestLoader(symbols));
      notifier.addLoaderIfNot(mode, const _TestLoader(symbols));
      await Future.delayed(Duration.zero);

      expect(notified, 1);
      expect(loadedSymbols, symbols);
    },
  );
}

class _TestLoader extends AbstractSymbolsLoader {
  final List<String> symbols;

  const _TestLoader(this.symbols);

  @override
  Future<SymbolsDictionary> get future async {
    return SymbolsDictionary(symbols: symbols);
  }
}
