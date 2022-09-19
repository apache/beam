/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:playground_components/dismissible_overlay.dart';

import '../../constants/sizes.dart';
import 'sign_in_overlay_content.dart';

class SignInButton extends StatefulWidget {
  const SignInButton();

  @override
  State<SignInButton> createState() => _SignInButtonState();
}

class _SignInButtonState extends State<SignInButton> {
  @override
  Widget build(BuildContext context) {
    return TextButton(
      onPressed: _openOverlay,
      child: const Text('ui.signIn').tr(),
    );
  }

  void _openOverlay() {
    OverlayEntry? overlay;
    overlay = OverlayEntry(
      builder: (context) => DismissibleOverlay(
        close: () {
          overlay?.remove();
        },
        child: const Positioned(
          right: TobSizes.size10,
          top: TobSizes.appBarHeight,
          child: SignInOverlayContent(),
        ),
      ),
    );
    Overlay.of(context)?.insert(overlay);
  }
}
