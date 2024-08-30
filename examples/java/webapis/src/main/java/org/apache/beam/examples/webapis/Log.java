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
package org.apache.beam.examples.webapis;

import java.time.Instant;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link Log} has convenience {@link DoFn}s for logging output. */
class Log {
  private static final Logger LOG = LoggerFactory.getLogger(Log.class);

  /** Logs output to stderr. */
  static <T> ParDo.SingleOutput<T, T> errorOf() {
    return ParDo.of(new LogErrorFn<>());
  }

  /** Logs output to stdout. */
  static <T> ParDo.SingleOutput<T, T> infoOf() {
    return ParDo.of(new LogInfoFn<>());
  }

  private static class LogErrorFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void process(@Element T element, OutputReceiver<T> receiver) {
      LOG.error("{}: {}", Instant.now(), element);
      receiver.output(element);
    }
  }

  private static class LogInfoFn<T> extends DoFn<T, T> {

    @ProcessElement
    public void process(@Element T element, OutputReceiver<T> receiver) {
      LOG.info("{}: {}", Instant.now(), element);
      receiver.output(element);
    }
  }
}
