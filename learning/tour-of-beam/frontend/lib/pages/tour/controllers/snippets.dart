import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';

import '../../../cache/unit_progress.dart';

class UnitSnippetsController extends ChangeNotifier {
  UnitSnippetsController() {
    final unitsProgressCache = GetIt.instance.get<UnitsProgressCache>();
    unitsProgressCache.addListener(notifyListeners);
  }

  final _unitSnippets = <String, String?>{};

  Map<String, String?> getUnitSnippets() {
    final unitsProgressCache = GetIt.instance.get<UnitsProgressCache>();

    _unitSnippets.clear();
    for (final unitProgress in unitsProgressCache.getUnitsProgress()) {
      _unitSnippets[unitProgress.id] = unitProgress.userSnippetId;
    }

    print([
      'got snippets',
      _unitSnippets,
    ]);

    return _unitSnippets;
  }
}
