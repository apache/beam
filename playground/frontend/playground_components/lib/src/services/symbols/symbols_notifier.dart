import 'dart:async';

import 'package:flutter/widgets.dart';
import 'package:highlight/highlight_core.dart';

import '../../models/symbols_dictionary.dart';
import 'loaders/abstract.dart';

class SymbolsNotifier extends ChangeNotifier {
  final _dictionaryFuturesByByMode = <Mode, Future<SymbolsDictionary>>{};
  final _dictionariesByMode = <Mode, SymbolsDictionary>{};

  void addLoader(Mode mode, AbstractSymbolsLoader loader) {
    unawaited(_load(mode, loader));
  }

  Future<SymbolsDictionary> _load(
    Mode mode,
    AbstractSymbolsLoader loader,
  ) async {
    final future = _dictionaryFuturesByByMode[mode];

    if (future != null) {
      return future;
    }

    _dictionaryFuturesByByMode[mode] = loader.future;

    final dictionary = await loader.future;
    _dictionariesByMode[mode] = dictionary;
    notifyListeners();
    return dictionary;
  }

  SymbolsDictionary? getDictionary(Mode mode) => _dictionariesByMode[mode];
}
