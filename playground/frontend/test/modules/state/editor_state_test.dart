import 'package:playground/constants/sdk.dart';
import 'package:playground/modules/editor/state/editor_state.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('Editor State initial value should be java', () {
    final state = EditorState();
    expect(state.sdk, equals(SDK.java));
  });

  test('Editor state should notify all listeners about sdk change', () {
    final state = EditorState();
    state.addListener(() {
      expect(state.sdk, SDK.go);
    });
    state.setSdk(SDK.go);
  });
}
