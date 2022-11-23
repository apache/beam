import 'package:flutter_test/flutter_test.dart';
import 'package:playground_components/src/controllers/snippet_editing_controller.dart';
import 'package:playground_components/src/models/sdk.dart';

import '../common/examples.dart';

void main() {
  group(
    'Snippet editing controllers',
    () {
      test(
        'Returns standard descriptor if code has not been changed',
        () {
          final controller = SnippetEditingController(sdk: Sdk.python);
          controller.selectedExample = exampleMock1;

          final descriptor = controller.getLoadingDescriptor();

          expect(descriptor.toJson(), {'example': 'SDK_PYTHON/Category/Name1'});
        },
      );

      test(
        'Returns content descriptor if code has been changed',
        () {
          final controller = SnippetEditingController(sdk: Sdk.python);
          controller.selectedExample = exampleMock1;

          controller.codeController.value = const TextEditingValue(text: 'ex4');
          final descriptor = controller.getLoadingDescriptor();

          expect(descriptor.toJson(), {
            'sdk': 'python',
            'content': 'ex4',
            'name': 'Example X1',
            'complexity': 'basic'
          });
        },
      );

      test(
        'Returns standard descriptor if example has been changed',
        () {
          final controller = SnippetEditingController(sdk: Sdk.python);
          controller.selectedExample = exampleMock1;
          
          controller.selectedExample = exampleMock2;
          final descriptor = controller.getLoadingDescriptor();

          expect(descriptor.toJson(), {'example': 'SDK_PYTHON/Category/Name2'});
        },
      );
    },
  );
}
