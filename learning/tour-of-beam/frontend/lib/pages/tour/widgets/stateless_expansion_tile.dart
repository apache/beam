import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';

import '../../../components/expansion_tile_wrapper.dart';

class StatelessExpansionTile extends StatelessWidget {
  final bool isExpanded;
  final void Function(bool)? onExpansionChanged;
  final Widget title;
  final Widget child;

  const StatelessExpansionTile({
    required super.key,
    required this.isExpanded,
    required this.onExpansionChanged,
    required this.title,
    required this.child,
  });

  @override
  Widget build(BuildContext context) {
    return ExpansionTileWrapper(
      ExpansionTile(
        // TODO(nausharipov): isn't rebuilt without key.
        key: key,
        initiallyExpanded: isExpanded,
        tilePadding: EdgeInsets.zero,
        onExpansionChanged: onExpansionChanged,
        title: title,
        childrenPadding: const EdgeInsets.only(
          left: BeamSizes.size24,
        ),
        children: [child],
      ),
    );
  }
}
