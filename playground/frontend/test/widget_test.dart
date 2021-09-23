import 'package:flutter_test/flutter_test.dart';

import 'package:playground/main.dart';

void main() {
  testWidgets('Home Page', (WidgetTester tester) async {
    // Build our app and trigger a frame.
    await tester.pumpWidget(const PlaygroundApp());

    // Verify that Playground text is displayed
    expect(find.text('Playground'), findsOneWidget);
  });
}
