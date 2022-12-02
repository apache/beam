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
import 'package:flutter_svg/svg.dart';
import 'package:get_it/get_it.dart';

import '../../../assets/assets.gen.dart';
import '../../../solution.dart';

class SolutionButton extends StatelessWidget {
  const SolutionButton();

  @override
  Widget build(BuildContext context) {
    final solutionNotifier = GetIt.instance.get<SolutionNotifier>();

    return AnimatedBuilder(
      animation: solutionNotifier,
      builder: (context, child) => TextButton.icon(
        style: ButtonStyle(
          backgroundColor: MaterialStateProperty.all(
            solutionNotifier.showSolution
                ? Theme.of(context).splashColor
                : null,
          ),
        ),
        onPressed: solutionNotifier.toggleShowSolution,
        icon: SvgPicture.asset(Assets.svg.solution),
        label: const Text('ui.solution').tr(),
      ),
    );
  }
}
