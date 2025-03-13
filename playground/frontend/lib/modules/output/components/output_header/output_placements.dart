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
import 'package:flutter_gen/gen_l10n/app_localizations.dart';
import 'package:flutter_svg/flutter_svg.dart';
import 'package:playground/constants/sizes.dart';
import 'package:playground/modules/output/models/output_placement.dart';
import 'package:playground/modules/output/models/output_placement_state.dart';
import 'package:provider/provider.dart';

class OutputPlacements extends StatelessWidget {
  const OutputPlacements({Key? key}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<OutputPlacementState>(
      builder: (context, state, child) {
        return Padding(
          padding: const EdgeInsets.symmetric(horizontal: kXlSpacing),
          child: Wrap(
            spacing: kMdSpacing,
            children: OutputPlacement.values
                .map(
                  (placement) => Semantics(
                    label:
                        '${AppLocalizations.of(context)!.outputPlacementSemantic}'
                        ' ${placement.name(context)}',
                    child: IconButton(
                      key: ValueKey(placement),
                      splashRadius: kIconButtonSplashRadius,
                      icon: SvgPicture.asset(
                        placement.icon,
                        color: state.placement == placement
                            ? Theme.of(context).primaryColor
                            : null,
                      ),
                      onPressed: () => state.setPlacement(placement),
                    ),
                  ),
                )
                .toList(),
          ),
        );
      },
    );
  }
}
