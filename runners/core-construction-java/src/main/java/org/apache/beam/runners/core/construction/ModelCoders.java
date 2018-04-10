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

package org.apache.beam.runners.core.construction;

import static org.apache.beam.runners.core.construction.UrnUtils.validateCommonUrn;

/** Utilities and constants ot interact with coders that are part of the Beam Model. */
public class ModelCoders {
  private ModelCoders() {}

  public static final String BYTES_CODER_URN = validateCommonUrn("beam:coder:bytes:v1");
  // Where is this required explicitly, instead of implicit within WindowedValue and LengthPrefix
  // coders?
  public static final String INT64_CODER_URN = validateCommonUrn("beam:coder:varint:v1");

  public static final String ITERABLE_CODER_URN = validateCommonUrn("beam:coder:iterable:v1");
  public static final String KV_CODER_URN = validateCommonUrn("beam:coder:kv:v1");
  public static final String LENGTH_PREFIX_CODER_URN =
      validateCommonUrn("beam:coder:length_prefix:v1");

  public static final String GLOBAL_WINDOW_CODER_URN =
      validateCommonUrn("beam:coder:global_window:v1");
  // This isn't strictly required once there's a way to represent an 'unknown window' (i.e. the
  // custom window encoding + the maximum timestamp of the window, this can be used for interval
  // windows.
  public static final String INTERVAL_WINDOW_CODER_URN =
      validateCommonUrn("beam:coder:interval_window:v1");

  public static final String WINDOWED_VALUE_CODER_URN =
      validateCommonUrn("beam:coder:windowed_value:v1");
}
