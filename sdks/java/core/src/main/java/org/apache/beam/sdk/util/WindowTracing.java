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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logging for window operations. Generally only feasible to enable on hand-picked pipelines. */
@Internal
public final class WindowTracing {
  private static final Logger LOG = LoggerFactory.getLogger(WindowTracing.class);

  public static void debug(String format, Object... args) {
    LOG.debug(format, args);
  }

  @SuppressWarnings("unused")
  public static void trace(String format, Object... args) {
    LOG.trace(format, args);
  }
}
