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
package org.apache.beam.sdk.fn.server;

import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.BindableService;

/** An interface sharing common behavior with services used during execution of user Fns. */
public interface FnService extends AutoCloseable, BindableService {
  /**
   * {@inheritDoc}.
   *
   * <p>There should be no more calls to any service method by the time a call to {@link #close()}
   * begins. Specifically, this means that a {@link
   * org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server} that this service is bound to should have
   * completed a call to the {@link org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server#shutdown()}
   * method, and all future incoming calls will be rejected.
   */
  @Override
  void close() throws Exception;
}
