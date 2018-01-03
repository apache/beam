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

package org.apache.beam.sdk.nexmark.sources.synthetic;

import static org.apache.beam.sdk.nexmark.NexmarkUtils.standardGeneratorConfig;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.synthetic.generator.GeneratorCheckpoint;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Generates synthetic events.
 */
public class SyntheticEventsSource extends PTransform<PBegin, PCollection<Event>> {

  /**
   * Return a source of synthetic events. Events are generated in batch or streaming fasion,
   * based on the configuration option.
   */
  public static PTransform<PBegin, PCollection<Event>> create(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      String queryName) {

    NexmarkUtils.console("Generating %d events in %s mode",
        configuration.numEvents, options.isStreaming() ? "streaming" : "batch");

    String transformName = transformName(queryName, options.isStreaming());
    PTransform<PBegin, PCollection<Event>> transform = options.isStreaming()
        ? Read.from(streamEventsSource(configuration))
        : Read.from(batchEventsSource(configuration));

    return new SyntheticEventsSource(transformName, transform);
  }

  private static String transformName(String queryName, boolean isStreaming) {
    return queryName + (isStreaming ? ".ReadUnbounded" : ".ReadBounded");
  }

  private PTransform<PBegin, PCollection<Event>> delegateEventSource;

  private SyntheticEventsSource(String name,
                                PTransform<PBegin, PCollection<Event>> delegateEventSource) {
    super(name);
    this.delegateEventSource = delegateEventSource;
  }

  @Override
  public PCollection<Event> expand(PBegin input) {
    return input.apply(getName(), delegateEventSource);
  }

  /**
   * Return a source which yields a finite number of synthesized events generated as a batch.
   */
  public static BoundedSource<Event> batchEventsSource(NexmarkConfiguration configuration) {
    return new BoundedEventSource(
        standardGeneratorConfig(configuration),
        configuration.numEventGenerators);
  }

  /**
   * Return a source which yields a finite number of synthesized events generated
   * on-the-fly in real time.
   */
  public static UnboundedSource<Event, GeneratorCheckpoint> streamEventsSource(
      NexmarkConfiguration configuration) {

    return new UnboundedEventSource(
        standardGeneratorConfig(configuration),
        configuration.numEventGenerators,
        configuration.watermarkHoldbackSec,
        configuration.isRateLimited);
  }
}
