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
package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;

/**
 * Query "14" RESHUFFLE (not in original suite).
 *
 * <p>This benchmark is created to stress the reshuffle transform.
 */
public class Query14 extends NexmarkQueryTransform<Event> {
  private final NexmarkConfiguration configuration;

  public Query14(NexmarkConfiguration configuration) {
    super("Query14");
    this.configuration = configuration;
  }

  @Override
  public PCollection<Event> expand(PCollection<Event> events) {
    return events.apply(
        Reshuffle.<Event>viaRandomKey().withNumBuckets(configuration.numKeyBuckets));
  }
}
