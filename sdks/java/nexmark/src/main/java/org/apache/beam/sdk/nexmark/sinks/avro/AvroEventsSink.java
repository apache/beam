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

package org.apache.beam.sdk.nexmark.sinks.avro;

import com.google.common.base.Strings;

import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * Sink for Events to Avro files.
 * Note that this does not sink the query execution results but the Events themselves.
 * See {@link org.apache.beam.sdk.nexmark.NexmarkLauncher} for details.
 */
public class AvroEventsSink extends PTransform<PCollection<Event>, PCollection<Event>> {

  /**
   * Sink all raw Events in {@code source} to {@code options.getOutputPath}.
   * This will configure the job to write the following files:
   * <ul>
   * <li>{@code $outputPath/event*.avro} All Event entities.
   * <li>{@code $outputPath/auction*.avro} Auction entities.
   * <li>{@code $outputPath/bid*.avro} Bid entities.
   * <li>{@code $outputPath/person*.avro} Person entities.
   * </ul>
   */
  public static PTransform<PCollection<Event>, PCollection<Event>> createSink(
      NexmarkOptions options, String queryName) {

    String filename = options.getOutputPath();
    if (Strings.isNullOrEmpty(filename)) {
      throw new RuntimeException("Missing --outputPath");
    }

    NexmarkUtils.console("Writing events to Avro files at %s", filename);
    return new AvroEventsSink(filename, queryName);
  }

  private String filename;
  private String queryName;

  private AvroEventsSink(String filename, String queryName) {
    this.filename = filename;
    this.queryName = queryName;
  }

  @Override
  public PCollection<Event> expand(PCollection<Event> source) {
    source
        .apply(queryName + ".WriteAvroEvents", write(Event.class, "/event"));

    source
        .apply(NexmarkQuery.JUST_BIDS)
        .apply(queryName + ".WriteAvroBids", write(Bid.class, "/bid"));

    source
        .apply(NexmarkQuery.JUST_NEW_AUCTIONS)
        .apply(queryName + ".WriteAvroAuctions", write(Auction.class, "/auction"));

    source.apply(NexmarkQuery.JUST_NEW_PERSONS)
        .apply(queryName + ".WriteAvroPeople", write(Person.class, "/person"));

    return source;
  }

  private AvroIO.Write write(Class clazz, String subfolder) {
    return AvroIO
        .write(clazz)
        .to(filename + subfolder)
        .withSuffix(".avro");
  }
}
