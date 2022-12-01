import 'package:flutter/material.dart';

import '../../../playground_components.dart';

class OverlayBody extends StatelessWidget {
  final Widget child;

  const OverlayBody({required this.child});

  @override
  Widget build(BuildContext context) {
    return Material(
      elevation: BeamSizes.size10,
      borderRadius: BorderRadius.circular(BeamSizes.size10),
      child: child,
    );
  }
}
