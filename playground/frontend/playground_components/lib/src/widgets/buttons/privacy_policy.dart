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

import '../../constants/links.dart';
import 'text_external_url_navigation.dart';

class PrivacyPolicyButton extends StatelessWidget {
  const PrivacyPolicyButton();

  @override
  Widget build(BuildContext context) {
    return TextExternalUrlNavigationButton(
      title: 'ui.privacyPolicy'.tr(),
      url: Uri.parse(BeamLinks.privacyPolicy),
    );
  }
}
