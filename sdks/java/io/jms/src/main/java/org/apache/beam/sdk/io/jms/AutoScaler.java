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

import java.io.Serializable;
import org.apache.beam.sdk.io.UnboundedSource;

/**
 * Enables users to specify their own `JMS` backlog reporters enabling {@link JmsIO} to report
 * {@link UnboundedSource.UnboundedReader#getTotalBacklogBytes()}.
 */
public interface AutoScaler extends Serializable {

  /** The {@link AutoScaler} is started when the {@link JmsIO.UnboundedJmsReader} is started. */
  void start();

  /**
   * Returns the size of the backlog of unread data in the underlying data source represented by all
   * splits of this source.
   */
  long getTotalBacklogBytes();

  /** The {@link AutoScaler} is stopped when the {@link JmsIO.UnboundedJmsReader} is closed. */
  void stop();
}
