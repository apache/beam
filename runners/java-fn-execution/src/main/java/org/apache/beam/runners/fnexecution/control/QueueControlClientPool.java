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
package org.apache.beam.runners.fnexecution.control;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

/** Control client pool backed by a blocking queue. */
public class QueueControlClientPool<T extends InstructionRequestHandler>
    implements ControlClientPool {

  private final BlockingQueue<T> queue;

  /** Creates a client pool backed by a {@link SynchronousQueue}. */
  public static QueueControlClientPool createSynchronous() {
      return new QueueControlClientPool<>(new SynchronousQueue<>(true));
  }

  /** Creates a client pool backed by an unbounded {@link LinkedBlockingQueue}. */
  public static QueueControlClientPool createLinked() {
      return new QueueControlClientPool<>(new LinkedBlockingQueue<>());
  }

  private QueueControlClientPool(BlockingQueue<T> queue) {
      this.queue = queue;
  }

  @Override
  public ClientSource<T> getSource() {
    return queue::take;
  }

  @Override
  public ClientSink<T> getSink() {
      return queue::put;
  }

}
