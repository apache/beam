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
import 'package:flutter_svg/svg.dart';
import 'package:playground_components/playground_components.dart';
import 'package:url_launcher/url_launcher.dart';

class LinkButton extends StatelessWidget {
  final Color? color;
  final String iconPath;
  final bool showText;
  final String text;
  final String url;

  const LinkButton({
    required this.iconPath,
    required this.text,
    required this.url,
    this.color,
    this.showText = true,
  });

  @override
  Widget build(BuildContext context) {
    final icon = SvgPicture.asset(
      iconPath,
      color: color,
    );

    if (showText) {
      return TextButton.icon(
        icon: icon,
        onPressed: _onTap,
        label: Text(text),
      );
    } else {
      return InkWell(
        onTap: _onTap,
        borderRadius: const BorderRadius.all(Radius.circular(8)),
        child: Padding(
          padding: const EdgeInsets.all(2.0),
          child: icon,
        ),
      );
    }
  }

  void _onTap() {
    final uri = Uri.parse(url);

    launchUrl(uri);
    PlaygroundComponents.analyticsService.sendUnawaited(
      ExternalUrlNavigatedAnalyticsEvent(url: uri),
    );
  }
}
