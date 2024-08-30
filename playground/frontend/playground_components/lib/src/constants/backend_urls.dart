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

// ignore_for_file: prefer_interpolation_to_compose_strings

/// The template for production backend URL.
const defaultBackendUrlTemplate = 'https://{node}.play.beam.apache.org';

/// The URLs for local backend development.
const backendUrlOverrides = <String, String>{
  // Uncomment the following lines to use staging backend:
  // 'router': 'https://router.play-dev.beam.apache.org',
  // 'go': 'https://go.play-dev.beam.apache.org',
  // 'java': 'https://java.play-dev.beam.apache.org',
  // 'python': 'https://python.play-dev.beam.apache.org',
  // 'scio': 'https://scio.play-dev.beam.apache.org',

  // Uncomment the following lines to use local backend:
  // 'router': 'http://localhost:8081',
  // 'go': 'http://localhost:8084',
  // 'java': 'http://localhost:8086',
  // 'python': 'http://localhost:8088',
  // 'scio': 'http://localhost:8090',
};

/// The URL templates that will not be probed
/// if they are generated as the result of auto-composing backend URLs.
///
/// If any new project is created that uses the Playground Backend,
/// its pattern should be added to this list.
/// Otherwise the first backend candidate will be derived from
/// such project's frontend host and will be probed in vain on every page load.
final skipBackendUrls = [
  'tour.beam.apache.org'
].map((host) => RegExp(r'^http(s?)://(\w+)\.' + RegExp.escape(host) + r'$'));
