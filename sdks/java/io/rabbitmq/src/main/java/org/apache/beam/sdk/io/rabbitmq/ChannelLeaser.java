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
package org.apache.beam.sdk.io.rabbitmq;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.UUID;

/**
 * A Channel is responsible for maintaining delivery state between client and server along a
 * Connection. It's difficult to manage a Channel across different objects safely, so this 'loan
 * pattern' is present to ensure safe, consistent access.
 *
 * <p>This interface uses the concept of a 'lessee', which is the identifier of a particular client,
 * or set of clients, for a Channel. It is required that there be an "at-most-one" relationship
 * between lessees and Channels; that is, any given id will have zero or one Channel associated with
 * it.
 */
public interface ChannelLeaser {
  /**
   * A 'loan pattern' for utilizing a Channel such that the implementer handles exceptions and
   * tracks the state of the Channel consistently, freeing the caller to exclusively worry about
   * 'happy path' functionality.
   */
  @FunctionalInterface
  interface UseChannelFunction<T> {
    T apply(Channel channel) throws IOException;
  }

  /**
   * For a given lessee, applies the supplied function against the associated Channel. The lessee
   * need not worry about the state of the Channel based on the thrown Exception; the {@code
   * ChannelLeaser} implementation will be responsible for managing Connection/Channel state.
   */
  <T> T useChannel(UUID lessee, UseChannelFunction<T> f) throws IOException;

  /**
   * Explicitly close a Channel for a given lessee. If the lessee has no such channel or does not
   * exist, this will have no effect but will no throw an exception.
   */
  void closeChannel(UUID lessee);
}
