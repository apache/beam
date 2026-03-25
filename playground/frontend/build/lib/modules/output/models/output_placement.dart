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

import 'package:flutter/cupertino.dart';
import 'package:flutter_gen/gen_l10n/app_localizations.dart';

import '../../../src/assets/assets.gen.dart';

enum OutputPlacement {
  right,
  left,
  bottom,
  ;

  Axis get graphDirection {
    return this == OutputPlacement.bottom ? Axis.horizontal : Axis.vertical;
  }
}

extension OutputPlacementToIcon on OutputPlacement {
  String get icon {
    switch (this) {
      case OutputPlacement.bottom:
        return Assets.outputBottom;
      case OutputPlacement.right:
        return Assets.outputRight;
      case OutputPlacement.left:
        return Assets.outputLeft;
    }
  }
}

extension OutputPlacementName on OutputPlacement {
  String name(BuildContext context) {
    AppLocalizations appLocale = AppLocalizations.of(context)!;

    switch (this) {
      case OutputPlacement.bottom:
        return appLocale.bottom;
      case OutputPlacement.right:
        return appLocale.right;
      case OutputPlacement.left:
        return appLocale.left;
    }
  }
}
