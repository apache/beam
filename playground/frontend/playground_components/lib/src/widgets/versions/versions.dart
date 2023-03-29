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

import 'dart:math';

import 'package:easy_localization/easy_localization.dart';
import 'package:flutter/material.dart';
import 'package:get_it/get_it.dart';
import 'package:url_launcher/link.dart';

import '../../constants/sizes.dart';
import '../../controllers/build_metadata.dart';
import '../../models/component_version.dart';
import '../../models/sdk.dart';
import '../loading_indicator.dart';

const _commitHashLength = 8;

/// Shows versions of frontend, router, and [sdks] runners.
class VersionsWidget extends StatelessWidget {
  const VersionsWidget({
    required this.sdks,
  });

  final List<Sdk> sdks;

  @override
  Widget build(BuildContext context) {
    final controller = GetIt.instance.get<BuildMetadataController>();

    return AnimatedBuilder(
      animation: controller,
      builder: (context, child) => Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: [
          //
          _ComponentVersionWidget(
            Future.value(ComponentVersion.frontend),
            title: 'widgets.versions.titles.frontend'.tr(),
          ),

          const SizedBox(height: BeamSizes.size10),
          _ComponentVersionWidget(
            controller.getRouterVersion(), // ignore: discarded_futures
            title: 'widgets.versions.titles.router'.tr(),
          ),

          for (final sdk in sdks) ...[
            const SizedBox(height: BeamSizes.size10),
            _ComponentVersionWidget(
              controller.getRunnerVersion(sdk), // ignore: discarded_futures
              title: 'widgets.versions.titles.runner'.tr(
                namedArgs: {
                  'sdk': sdk.title,
                },
              ),
            ),
          ],
        ],
      ),
    );
  }
}

/// A line in [VersionsWidget].
class _ComponentVersionWidget extends StatelessWidget {
  const _ComponentVersionWidget(
    this.componentVersionFuture, {
    required this.title,
  });

  final Future<ComponentVersion> componentVersionFuture;
  final String title;

  @override
  Widget build(BuildContext context) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      mainAxisSize: MainAxisSize.min,
      children: [
        Text(title),
        _getContent(),
      ],
    );
  }

  Widget _getContent() {
    return FutureBuilder<ComponentVersion>(
      future: componentVersionFuture,
      builder: _getContentWithSnapshot,
    );
  }

  Widget _getContentWithSnapshot(
    BuildContext context,
    AsyncSnapshot<ComponentVersion> snapshot,
  ) {
    final data = snapshot.data;

    if (data == null) {
      return const LoadingIndicator();
    }

    final hash = data.buildCommitHash;
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      mainAxisSize: MainAxisSize.min,
      children: [
        //
        if (data.beamSdkVersion != null)
          const Text('widgets.versions.beam').tr(
            namedArgs: {
              'version': data.beamSdkVersion!,
            },
          ),

        if (hash != null)
          Link(
            uri: _commitHashToUri(data.buildCommitHash!),
            builder: (context, followLink) {
              return TextButton(
                onPressed: followLink,
                child: const Text('widgets.versions.commit').tr(
                  namedArgs: {
                    'hash': hash.substring(
                      0,
                      min(_commitHashLength, hash.length),
                    ),
                    'date': _formatDate(data.dateTime!),
                  },
                ),
              );
            },
          ),
      ],
    );
  }

  String _formatDate(DateTime dt) {
    return dt.toString().substring(0, 'YYYY-MM-DD HH:MM'.length);
  }
}

Uri _commitHashToUri(String commitHash) {
  return Uri.parse('https://github.com/apache/beam/tree/$commitHash');
}
