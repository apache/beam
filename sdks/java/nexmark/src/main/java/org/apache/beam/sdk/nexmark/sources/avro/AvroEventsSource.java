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

package org.apache.beam.sdk.nexmark.sources.avro;

import com.google.common.base.Strings;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Reads events from Avro files.
 */
public class AvroEventsSource extends PTransform<PBegin, PCollection<Event>> {

  /**
   * Return Avro source of events from {@code options.getInputFilePrefix}.
   */
  public static PTransform<PBegin, PCollection<Event>> createSource(
      NexmarkOptions options,
      String queryName) {

    String filename = options.getInputPath();
    if (Strings.isNullOrEmpty(filename)) {
      throw new RuntimeException("Missing --inputPath");
    }

    NexmarkUtils.console("Reading events from Avro files at %s", filename);

    return new AvroEventsSource(queryName, filename);
  }

  private String queryName;
  private String filename;

  private AvroEventsSource(String queryName, String filename) {
    this.queryName = queryName;
    this.filename = filename;
  }

  @Override
  public PCollection<Event> expand(PBegin input) {
    return input
        .apply(queryName + ".ReadAvroEvents", readEventsFrom(filename))
        .apply("OutputWithTimestamp", NexmarkQuery.EVENT_TIMESTAMP_FROM_DATA);
  }

  private static AvroIO.Read<Event> readEventsFrom(String filename) {
    return AvroIO.read(Event.class).from(filename + "*.avro");
  }
}
