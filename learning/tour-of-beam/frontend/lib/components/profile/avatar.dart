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

import 'package:firebase_auth/firebase_auth.dart';
import 'package:flutter/material.dart';
import 'package:playground_components/playground_components.dart';

import 'user_menu.dart';

class Avatar extends StatelessWidget {
  final User user;
  const Avatar({required this.user});

  @override
  Widget build(BuildContext context) {
    final photoUrl = user.photoURL;
    return GestureDetector(
      onTap: () {
        final closeNotifier = PublicNotifier();
        showOverlay(
          context: context,
          closeNotifier: closeNotifier,
          positioned: Positioned(
            right: BeamSizes.size10,
            top: BeamSizes.appBarHeight,
            child: UserMenu(
              closeOverlayCallback: closeNotifier.notifyPublic,
              user: user,
            ),
          ),
        );
      },
      child: CircleAvatar(
        backgroundColor: BeamColors.white,
        foregroundImage: photoUrl == null ? null : NetworkImage(photoUrl),
        child: const Icon(
          Icons.person,
          color: BeamColors.grey3,
        ),
      ),
    );
  }
}
