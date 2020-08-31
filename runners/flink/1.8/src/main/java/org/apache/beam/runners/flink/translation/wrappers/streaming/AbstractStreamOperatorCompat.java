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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;

/** Compatibility layer for {@link AbstractStreamOperator} breaking changes. */
public abstract class AbstractStreamOperatorCompat<OutputT>
    extends AbstractStreamOperator<OutputT> {
  // timeServiceManager was made private behind a getter in Flink 1.11
  protected InternalTimeServiceManager<?> getTimeServiceManagerCompat() {
    return timeServiceManager;
  }
}
