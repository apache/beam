import 'package:app_state/app_state.dart';
import 'package:flutter/foundation.dart';
import 'package:playground/constants/params.dart';
import 'package:playground/modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import 'package:playground_components/playground_components.dart';

import 'screen.dart';
import 'state.dart';

class EmbeddedPlaygroundPage
    extends StatefulMaterialPage<void, EmbeddedPlaygroundNotifier> {
  static const classFactoryKey = 'EmbeddedPlayground';

  /// Called when navigating to the page programmatically.
  EmbeddedPlaygroundPage({
    required ExamplesLoadingDescriptor descriptor,
    required bool isEditable,
  }) : super(
          key: const ValueKey(classFactoryKey),
          state: EmbeddedPlaygroundNotifier(
            initialDescriptor: descriptor,
            isEditable: isEditable,
          ),
          createScreen: EmbeddedPlaygroundScreen.new,
        );

  /// Called when re-creating the page at a navigation intent.
  // ignore: avoid_unused_constructor_parameters
  EmbeddedPlaygroundPage.fromStateMap(Map map)
      : this(
          descriptor: ExamplesLoadingDescriptorFactory.fromMap(
            map['descriptor'] ?? {},
          ),
          isEditable: map[kIsEditableParam] == '1',
        );
}
