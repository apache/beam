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
package org.apache.beam.sdk.io.jms;

import static org.apache.beam.sdk.io.UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;

/**
 * Default implementation of {@link AutoScaler}. Returns {@link
 * org.apache.beam.sdk.io.UnboundedSource.UnboundedReader#BACKLOG_UNKNOWN} as the default value.
 */
public class DefaultAutoscaler implements AutoScaler {
  @Override
  public void start() {}

  @Override
  public long getTotalBacklogBytes() {
    return BACKLOG_UNKNOWN;
  }

  @Override
  public void stop() {}
}
