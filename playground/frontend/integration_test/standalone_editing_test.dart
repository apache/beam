import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:playground_components_dev/playground_components_dev.dart';

import 'common/common.dart';
import 'common/common_finders.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('Testing editing code', (WidgetTester wt) async {
    await init(wt);
    // await _checkHotkeys(wt);
    await _checkAutocomplete(wt);
    // await _editingAndResettingChanges(wt);
    // await _checkCodeHighlighting(wt);
    // await _codeBlockFoldingTest(wt);
  });
}

Future<void> _checkAutocomplete(WidgetTester wt) async {
  final codeController = wt.findOneCodeController();
  final sSuggestions = await codeController.autocompleter.getSuggestions('s');
  expect(sSuggestions, ['short', 'static', 'strictfp', 'super', 'switch', 'synchronized']);
}

Future<void> _editingAndResettingChanges(WidgetTester wt) async {
  final playgroundController = wt.findPlaygroundController();

  final code = playgroundController.source;

  expect(code, isNotNull);

  await wt.tapAndSettle(find.resetButton());

  expect(playgroundController.source == code, true);

  await wt.enterText(find.codeField(), 'print("Hello World!');
  await wt.pumpAndSettle();

  expect(playgroundController.source != code, true);

  await wt.tapAndSettle(find.resetButton());

  expect(playgroundController.source, equals(code));
}

Future<void> _checkCodeHighlighting(WidgetTester wt) async {
  final codeController = wt.findOneCodeController();
  final colors = <Color>{};
  var textSpan = codeController.lastTextSpan;
  _collectTextSpanTreeTextColors(textSpan, colors);

  expect(colors.length, greaterThan(1));
}

void _collectTextSpanTreeTextColors(InlineSpan? span, Set<Color> colors) {
  if (span is TextSpan) {
    if (span.style?.color != null) {
      colors.add(span.style!.color!);
    }
    if (span.children != null) {
      for (final child in span.children!) {
        _collectTextSpanTreeTextColors(child, colors);
      }
    }
  }
}

Future<void> _codeBlockFoldingTest(WidgetTester wt) async {
  const code = '''
public class MyClass {
  public static void main(String[] args) {
    System.out.print("Hello World!");
  }
}
''';

  await wt.enterText(find.codeField(), code);
  await wt.pumpAndSettle();

  await wt.tapAndSettle(_getTopToggle(wt));

  const foldedCode = '''
public class MyClass {
''';

  expect(wt.findOneCodeController().text, equals(foldedCode));

  await wt.tapAndSettle(_getFoldToggles());

  expect(wt.findOneCodeController().text, equals(code));
}

Finder _getTopToggle(WidgetTester wt) {
  Finder foldToggles = _getFoldToggles();

  Finder topToggle =
      wt.getCenter(foldToggles.at(0)).dy < wt.getCenter(foldToggles.at(1)).dy
          ? foldToggles.at(0)
          : foldToggles.at(1);
  return topToggle;
}

Finder _getFoldToggles() {
  Finder foldToggles = find.descendant(
    of: find.byType(RotatedBox),
    matching: find.byIcon(Icons.chevron_right),
  );
  return foldToggles;
}

// Future<void> _checkHotkeys(WidgetTester wt) async {
//   final undoKeys = <LogicalKeyboardKey>{
//     LogicalKeyboardKey.keyZ,
//     LogicalKeyboardKey.meta,
//   };

//   final redoKeys = <LogicalKeyboardKey>{
//     LogicalKeyboardKey.keyZ,
//     LogicalKeyboardKey.meta,
//     LogicalKeyboardKey.shift,
//   };

//   final playgroundController = wt.findPlaygroundController();

//   final code = playgroundController.source;

//   expect(code, isNotNull);

//   await wt.enterText(find.codeField(), 'print("Hello World!");');
//   await wt.pumpAndSettle();

//   expect(playgroundController.source != code, true);

//   await wt.runShortcut(undoKeys);

//   expect(playgroundController.source, equals(code));

//   await wt.runShortcut(redoKeys);

//   expect(playgroundController.source != code, true);
// }