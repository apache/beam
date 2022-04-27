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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options.RpcPriority;

/**
 * Single place for defining the constants used in the {@code Spanner.readChangeStreams()}
 * connector.
 */
public class ChangeStreamsConstants {

  /** Represents the max end at that can be specified for a change stream. */
  public static final Timestamp MAX_INCLUSIVE_END_AT =
      Timestamp.ofTimeSecondsAndNanos(
          Timestamp.MAX_VALUE.getSeconds(), Timestamp.MAX_VALUE.getNanos() - 1);

  /** The default change stream name for a change stream query is the empty {@link String}. */
  public static final String DEFAULT_CHANGE_STREAM_NAME = "";

  /** The default start timestamp for a change stream query is {@link Timestamp#MIN_VALUE}. */
  public static final Timestamp DEFAULT_INCLUSIVE_START_AT = Timestamp.MIN_VALUE;

  /**
   * The default end timestamp for a change stream query is {@link
   * ChangeStreamsConstants#MAX_INCLUSIVE_END_AT}.
   */
  public static final Timestamp DEFAULT_INCLUSIVE_END_AT = MAX_INCLUSIVE_END_AT;

  /** The default priority for a change stream query is {@link RpcPriority#HIGH}. */
  public static final RpcPriority DEFAULT_RPC_PRIORITY = RpcPriority.HIGH;
}
