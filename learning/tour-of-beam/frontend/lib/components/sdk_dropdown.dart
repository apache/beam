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
import 'package:playground_components/playground_components.dart';

class SdkDropdown extends StatelessWidget {
  const SdkDropdown();

  @override
  Widget build(BuildContext context) {
    return _DropdownWrapper(
      child: DropdownButton(
        value: 'Java',
        onChanged: (sdk) {
          // TODO(nausharipov): change SDK
        },
        items: const ['Java', 'Python', 'Go']
            .map(
              (sdk) => DropdownMenuItem(
                value: sdk,
                child: Text(sdk),
              ),
            )
            .toList(growable: false),
        isDense: true,
        alignment: Alignment.center,
        focusColor: BeamColors.transparent,
        borderRadius: BorderRadius.circular(BeamSizes.size6),
      ),
    );
  }
}

class _DropdownWrapper extends StatelessWidget {
  final Widget child;
  const _DropdownWrapper({required this.child});

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: BoxDecoration(
        color: Theme.of(context).hoverColor,
        borderRadius: BorderRadius.circular(BeamSizes.size6),
      ),
      child: DropdownButtonHideUnderline(child: child),
    );
  }
}
