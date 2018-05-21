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
package org.apache.beam.sdk.nexmark.sources;

import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TimestampedValue;

/** A DoFn to generate bounded event records. */
public class EventGeneratorDoFn extends DoFn<Void, Event> {
  /** Configuration we generate events against. */
  private final GeneratorConfig config;
  /** Generator we are reading from. */
  private transient Generator generator;

  public EventGeneratorDoFn(GeneratorConfig config) {
    this.config = config;
  }

  @Setup
  public void setup() {
    generator = new Generator(config);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    while (generator.hasNext()) {
      final TimestampedValue<Event> next = generator.next();
      c.outputWithTimestamp(next.getValue(), next.getTimestamp());
    }
  }
}
