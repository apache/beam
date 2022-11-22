import 'package:firebase_auth/firebase_auth.dart';
import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';

import 'login/content.dart';
import 'profile/user_menu.dart';

void openLoginOverlay(BuildContext context, [User? user]) {
  final overlayCloser = PublicNotifier();
  final overlay = OverlayEntry(
    builder: (context) {
      return DismissibleOverlay(
        close: overlayCloser.notifyPublic,
        child: Positioned(
          right: BeamSizes.size10,
          top: BeamSizes.appBarHeight,
          child: user == null
              ? LoginContent(
                  onLoggedIn: overlayCloser.notifyPublic,
                )
              : UserMenu(
                  onLoggedOut: overlayCloser.notifyPublic,
                  user: user,
                ),
        ),
      );
    },
  );
  overlayCloser.addListener(overlay.remove);
  Overlay.of(context)?.insert(overlay);
}
