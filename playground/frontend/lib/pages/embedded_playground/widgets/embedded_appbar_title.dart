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

import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_svg/flutter_svg.dart';
import 'package:playground_components/playground_components.dart';
import 'package:provider/provider.dart';

import '../../../components/playground_run_or_cancel_button.dart';
import '../../../constants/sizes.dart';
import '../../../src/assets/assets.gen.dart';

class EmbeddedAppBarTitle extends StatelessWidget {
  const EmbeddedAppBarTitle({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PlaygroundController>(
      builder: (context, controller, child) => Wrap(
        crossAxisAlignment: WrapCrossAlignment.center,
        spacing: kXlSpacing,
        children: [
          const PlaygroundRunOrCancelButton(),
          const ToggleThemeIconButton(),
          IconButton(
            iconSize: kIconSizeLg,
            splashRadius: kIconButtonSplashRadius,
            icon: SvgPicture.asset(Assets.copy),
            onPressed: () {
              final source = controller.source;
              if (source != null) {
                Clipboard.setData(ClipboardData(text: source));
              }
            },
          ),
        ],
      ),
    );
  }
}
