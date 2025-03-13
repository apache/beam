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

import 'dart:async';

import 'package:aligned_dialog/aligned_dialog.dart';
import 'package:flutter/material.dart';

import '../../constants/sizes.dart';
import '../../controllers/playground_controller.dart';
import 'result_filter_popover.dart';

class ResultFilterButton extends StatelessWidget {
  const ResultFilterButton({
    required this.playgroundController,
  });

  final PlaygroundController playgroundController;

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: () {
        unawaited(
          showAlignedDialog(
            context: context,
            builder: (dialogContext) => ResultFilterPopover(
              playgroundController: playgroundController,
            ),
            followerAnchor: Alignment.topLeft,
            targetAnchor: Alignment.topLeft,
            barrierColor: Colors.transparent,
          ),
        );
      },
      child: Icon(
        Icons.filter_alt_outlined,
        size: BeamIconSizes.small,
        color: Theme.of(context).primaryColor,
      ),
    );
  }
}
