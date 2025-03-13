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

int getDepth(String line) {
  final diff = line.length - line.trimLeft().length;
  return (diff / 4).floor();
}

String extractNodeId(String line) {
  return line.substring(0, line.indexOf(' '));
}

String extractNodeLabel(String line) {
  return line
      .substring(line.indexOf(' ') + 1)
      .replaceAll('[label="', '')
      .replaceAll('"]', '');
}
