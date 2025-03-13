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
import 'package:playground/modules/examples/models/popover_state.dart';
import 'package:provider/provider.dart';

class OutsideClickHandler extends StatelessWidget {
  final void Function() onTap;

  const OutsideClickHandler({Key? key, required this.onTap}) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Consumer<PopoverState>(builder: (context, state, child) {
      if (state.isOpen) {
        return Container();
      }
      return GestureDetector(
        onTap: onTap,
        child: Container(
          color: Colors.transparent,
          height: double.infinity,
          width: double.infinity,
        ),
      );
    });
  }
}
