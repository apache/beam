import 'package:playground/modules/sdk/models/sdk.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:playground/pages/playground/playground_state.dart';

void main() {
  test('Playground State initial value should be java', () {
    final state = PlaygroundState();
    expect(state.sdk, equals(SDK.java));
  });

  test('Playground state should notify all listeners about sdk change', () {
    final state = PlaygroundState();
    state.addListener(() {
      expect(state.sdk, SDK.go);
    });
    state.setSdk(SDK.go);
  });
}
