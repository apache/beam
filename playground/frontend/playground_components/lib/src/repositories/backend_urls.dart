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

// ignore_for_file: avoid_print

import '../../playground_components.dart';
import '../constants/backend_urls.dart';

const _routerNode = 'router';

Future<Uri> getRouterUrl() => _getBackendUrl(_routerNode);
Future<Uri> getRunnerUrl(Sdk sdk) => _getBackendUrl(sdk.id);

/// Returns the URL for the backend [node].
///
/// Calls [_getBackendUrlOptions] for options.
///
/// If only one option exists, returns it.
/// This ensures fast initialization in production.
///
/// If multiple options exist, each of them except the last one is probed
/// with getMetadata() call. This results in slower initialization for
/// custom stages but keeps the configuration simple.
Future<Uri> _getBackendUrl(String node) async {
  final urls = _getBackendUrlOptions(node);

  if (urls.length == 1) {
    return urls.first;
  }

  print('Probing multiple options for $node backend:');
  urls.forEach(print);

  final lastUrl = urls.removeLast();

  for (final url in urls) {
    try {
      final client = GrpcExampleClient(url: url);
      await client.getMetadata();
      print('Using $url');
      return url;
    } on Exception catch (ex) {
      print('$url failed');
      print(ex);
    }
  }

  print('Using $lastUrl');
  return lastUrl;
}

/// Returns options for backend URLs for [node].
///
/// If an override is given in [backendUrlOverrides], it is the only option.
/// This can be used for development if the production backend
/// must not be used.
///
/// Otherwise there are 2 options in the following order:
/// 1. [node] is prepended to the host of the current URL.
/// 2. [node] is inserted into [defaultBackendUrlTemplate] (the production URL).
/// If the two are the same, only one is returned.
/// This results in the following when looking up "node":
///
/// For production Playground:
/// - node.play.beam.apache.org (the production URL).
///
/// For production Tour of Beam:
/// - node.play.beam.apache.org (as node.tour.beam.apache.org is blacklisted
///   via [skipBackendUrls]).
///
/// For any other stage of Playground or Tour of Beam (my-stage.com):
/// - node.my-stage.com
/// - node.play.beam.apache.org
/// This means that if the stage runs its own container for the given
/// backend node, it is used. Otherwise the production backend is used.
List<Uri> _getBackendUrlOptions(String node) {
  final override = backendUrlOverrides[node];
  if (override != null) {
    return [Uri.parse(override)];
  }

  final currentHost = Uri.base.host;
  final nodeUriFromCurrentHost = Uri(
    scheme: Uri.base.scheme,
    host: '$node.$currentHost',
    port: Uri.base.port,
  );

  return {
    nodeUriFromCurrentHost.toString(),
    defaultBackendUrlTemplate.replaceAll('{node}', node),
  }.where(_shouldAttemptUrl).map(Uri.parse).toList();
}

/// Whether [url] does not match any pattern in [skipBackendUrls].
bool _shouldAttemptUrl(String url) {
  return !skipBackendUrls.any((pattern) => pattern.allMatches(url).isNotEmpty);
}
