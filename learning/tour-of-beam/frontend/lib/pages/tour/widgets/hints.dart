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
import 'package:playground_components/playground_components.dart';

import '../../../assets/assets.gen.dart';
import 'markdown/tob_markdown.dart';

class HintsWidget extends StatelessWidget {
  final List<String> hints;

  const HintsWidget({
    required this.hints,
  });

  @override
  Widget build(BuildContext context) {
    return TextButton.icon(
      onPressed: () {
        if (hints.isNotEmpty) {
          showDialog(
            context: context,
            builder: (context) => Dialog(
              backgroundColor: Colors.transparent,
              child: _Popup(hint: hints.first),
            ),
          );
        }
      },
      icon: SvgPicture.asset(Assets.svg.hint),
      label: const Text('pages.tour.hint').tr(),
    );
  }
}

class _Popup extends StatelessWidget {
  final String hint;

  const _Popup({
    required this.hint,
  });

  @override
  Widget build(BuildContext context) {
    return OverlayBody(
      child: Container(
        width: BeamSizes.popupWidth,
        padding: const EdgeInsets.all(BeamSizes.size16),
        child: SingleChildScrollView(
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            mainAxisSize: MainAxisSize.min,
            children: [
              Text(
                'pages.tour.hint',
                style: Theme.of(context).textTheme.headlineLarge,
              ).tr(),
              const SizedBox(height: BeamSizes.size8),
              TobMarkdown(
                padding: EdgeInsets.zero,
                data: hint,
              ),
            ],
          ),
        ),
      ),
    );
  }
}
