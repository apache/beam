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
package org.apache.beam.sdk.fn.data;

/**
 * A receiver of streamed data that can be closed.
 *
 * <p>The close method for a {@link CloseableFnDataReceiver} must be idempotent.
 */
public interface CloseableFnDataReceiver<T> extends FnDataReceiver<T>, AutoCloseable {

  /**
   * Eagerly flushes any data that is buffered in this channel.
   *
   * @deprecated to be removed once splitting/checkpointing are available in SDKs and rewinding in
   *     readers.
   * @throws Exception
   */
  @Deprecated
  void flush() throws Exception;

  /**
   * {@inheritDoc}.
   *
   * <p>Does nothing if this {@link CloseableFnDataReceiver} is already closed.
   */
  @Override
  void close() throws Exception;
}
