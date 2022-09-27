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

import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:playground_components/playground_components.dart';

import '../models/sdk_list.dart';

class SdkDropdown extends StatelessWidget {
  const SdkDropdown();

  // TODO(nausharipov): remove after demo
  Future<Map<String, dynamic>> _getSdks() async {
    final response = await http.get(
      Uri.parse(
        'https://us-central1-tour-of-beam-2.cloudfunctions.net/getSdkList',
      ),
    );
    final decodedResponse = jsonDecode(utf8.decode(response.bodyBytes));
    return decodedResponse;
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder(
      future: _getSdks(),
      builder: (context, snapshot) {
        if (!snapshot.hasData) return Container();
        final sdks = SdkListModel.fromJson(snapshot.data!).sdks;
        return _DropdownWrapper(
          child: DropdownButton(
            value: sdks.first.id,
            onChanged: (sdk) {
              // TODO(nausharipov): change SDK
            },
            items: sdks
                .map(
                  (sdk) => DropdownMenuItem(
                    value: sdk.id,
                    child: Text(sdk.title),
                  ),
                )
                .toList(growable: false),
            isDense: true,
            alignment: Alignment.center,
            focusColor: BeamColors.transparent,
            borderRadius: BorderRadius.circular(BeamSizes.size6),
          ),
        );
      },
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
