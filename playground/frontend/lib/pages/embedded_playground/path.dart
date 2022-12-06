import 'package:app_state/app_state.dart';
import 'package:flutter/widgets.dart';
import 'package:playground_components/playground_components.dart';

import '../../modules/examples/models/example_loading_descriptors/examples_loading_descriptor_factory.dart';
import '../../constants/params.dart';
import 'page.dart';

class EmbeddedPlaygroundPath extends PagePath {
  final ExamplesLoadingDescriptor descriptor;
  final bool isEditable;

  static const _location = '/embedded';

  EmbeddedPlaygroundPath({
    required this.descriptor,
    required this.isEditable,
  }) : super(
          key: EmbeddedPlaygroundPage.classFactoryKey,
          state: {'descriptor': descriptor.toJson()},
        );

  static EmbeddedPlaygroundPath? tryParse(RouteInformation ri) {
    final uri = Uri.parse(ri.location ?? '');
    if (uri.path != _location) {
      return null;
    }

    final descriptor = ExamplesLoadingDescriptorFactory.fromEmbeddedParams(
      uri.queryParameters,
    );

    return EmbeddedPlaygroundPath(
      descriptor: descriptor,
      isEditable: uri.queryParameters[kIsEditableParam] == '1',
    );
  }
}
