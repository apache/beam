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
import 'package:playground_components/constants/colors.dart';
import 'package:playground_components/constants/sizes.dart';
import 'package:playground_components/theme/color_provider.dart';
import 'package:playground_components/widgets/split_view.dart';

import '../../components/page_container.dart';
import '../../constants/sizes.dart';

class MainScreen extends StatelessWidget {
  const MainScreen();

  @override
  Widget build(BuildContext context) {
    return PageContainer(
      content: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const _Summary(),
          Flexible(
            child: SplitView(
              themeData: Theme.of(context),
              first: const _Content(),
              second: const _Playground(),
              direction: SplitViewDirection.horizontal,
            ),
          ),
        ],
      ),
    );
  }
}

class _Summary extends StatelessWidget {
  const _Summary();

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: BeamSizes.size12),
      child: Column(
        children: [
          Row(
            children: [
              Text(
                'Table of Contents',
              ),
              Text('<<'),
            ],
          ),
          Row(
            children: [
              Text(
                'Core Transforms',
              ),
              Text(
                '...',
              ),
            ],
          ),
          Row(
            children: [
              Row(
                children: [
                  Text(
                    'o',
                  ),
                  Text(
                    'Map',
                  ),
                ],
              ),
              Text(
                '^',
              ),
            ],
          ),
          Text(
            'ParDo one-to-one',
          ),
          Divider(),
        ],
      ),
    );
  }
}

class _Content extends StatelessWidget {
  const _Content();

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      // TODO(nausharipov): is there a simpler solution?
      height: MediaQuery.of(context).size.height -
          BeamSizes.appBarHeight -
          TobSizes.footerHeight,
      child: Column(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Expanded(
            child: Container(
              color: BeamColors.white,
              child: SingleChildScrollView(
                child: Text('Using Filter'),
              ),
            ),
          ),
          Container(
            decoration: BoxDecoration(
              border: Border(
                top: BorderSide(color: ThemeColors.of(context).divider),
              ),
              color: ThemeColors.of(context).secondaryBackground,
            ),
            padding: const EdgeInsets.all(20),
            child: Text('Complete Unit'),
          ),
        ],
      ),
    );
  }
}

class _Playground extends StatelessWidget {
  const _Playground();

  @override
  Widget build(BuildContext context) {
    return const Text('Playground');
  }
}
