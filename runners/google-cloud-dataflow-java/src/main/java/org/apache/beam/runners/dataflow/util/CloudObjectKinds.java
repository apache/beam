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
package org.apache.beam.runners.dataflow.util;

/** Known kinds of {@link CloudObject}. */
class CloudObjectKinds {
  static final String KIND_GLOBAL_WINDOW = "kind:global_window";
  static final String KIND_INTERVAL_WINDOW = "kind:interval_window";
  static final String KIND_CUSTOM_WINDOW = "kind:custom_window";
  static final String KIND_LENGTH_PREFIX = "kind:length_prefix";
  static final String KIND_PAIR = "kind:pair";
  static final String KIND_STREAM = "kind:stream";
  static final String KIND_WINDOWED_VALUE = "kind:windowed_value";
  static final String KIND_BYTES = "kind:bytes";
}
