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

class BeamCloseButton extends StatelessWidget {
  static const _width = 120.0;
  static const _height = 40.0;

  @override
  Widget build(BuildContext context) {
    return ElevatedButton(
      style: const ButtonStyle(
        elevation: MaterialStatePropertyAll<double>(0),
        fixedSize: MaterialStatePropertyAll<Size>(
          Size(_width, _height),
        ),
        shape: MaterialStatePropertyAll<StadiumBorder>(
          StadiumBorder(),
        ),
        padding: MaterialStatePropertyAll(EdgeInsets.only(bottom: 2)),
      ),
      onPressed: () => Navigator.of(context).pop(),
      child: Text('widgets.closeButton.label'.tr().toUpperCase()),
    );
  }
}
