import 'package:app_state/app_state.dart';
import 'package:flutter/material.dart';

import '../widgets/toasts/toast_listener.dart';

/// Wraps [pageStack] in widgets that must be above [Navigator] and can be
/// below [MaterialApp].
class BeamRouterDelegate extends PageStackRouterDelegate {
  BeamRouterDelegate(super.pageStack);

  @override
  Widget build(BuildContext context) {
    // Overlay: to float toasts.
    // ToastListenerWidget: turns notification events into floating toasts.
    return Overlay(
      initialEntries: [
        OverlayEntry(
          builder: (context) => ToastListenerWidget(
            child: super.build(context),
          ),
        ),
      ],
    );
  }
}
