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
package org.apache.beam.sdk.options.org.apache.beam.examples;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A sample triggered side input.
 *
 * <p>In this sample, a file contains a mapping of user ids to extra metadata about those users.
 * This file is periodically refreshed as the metadata changes and as new users enter the system.
 * The sample reads an unbounded stream of user events, enriches each event based on the latest
 * version of the metadata file, and outputs the enriched event.
 */
public class TriggeredSideInput {
  // Represents a user in the system.
  @DefaultCoder(AvroCoder.class)
  private class UserEvent {
    String userId;
    String userMetadata;
    String eventType; // view, click, etc.
  }

  // Represents a user metadata file. Extra information about each user. This
  // information changes periodically, and is loaded from a file.
  @DefaultCoder(AvroCoder.class)
  private static class UserMetadataFile {
    public Map<String, String> userMetadata;
    UserMetadataFile(Properties properties) {
      userMetadata = new HashMap<>();
      for (String user : properties.stringPropertyNames()) {
        userMetadata.put(user, properties.getProperty(user));
      }
    }
  }

  /**
   * Options for this sample.
   */
  public interface Options extends StreamingOptions {
    Long getPeriodMinutes();
    void setPeriodMinutes(Long period);

    Long getElementsPerPeriod();
    void setElementsPerPeriod(Long elementsPerPeriod);

    String getUserMetadataFilename();
    void setUserMetadataFilename(String userMetadataFilename);

    String getUserEventsTopic();
    void setUserEventsTopic(String userEventsTopic);

    String getEnrichedEventsTopic();
    void setEnrichedEventsTopic(String enrichedEventsTopic);
  }


  /**
   * A DoFn that reads in a UserMetadata file, and parses it into a {@link UserMetadataFile}
   * object.
   */
  private static class ReadFile extends DoFn<Long, TimestampedValue<UserMetadataFile>> {
    String filename;

    ReadFile(String filename) {
      this.filename = filename;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      // The file is expected to be in a Java Properties format
      // (each line is of the form key=value).
      Properties properties = new Properties();
      properties.load(new FileReader(filename));
      // We timestamp the element with the time we read the file, so we can keep
      // the most-recent file. (ideally we would use filesystem creation time instead).
      c.output(TimestampedValue.of(new UserMetadataFile(properties), Instant.now()));
    }
  }

  /**
   * A DoFn that enriches every user with the latest version of the metadata file.
   */
  private static class EnrichUserWithMetadata extends DoFn<UserEvent, UserEvent> {
    PCollectionView<TimestampedValue<UserMetadataFile>> latestFileView;

    EnrichUserWithMetadata(PCollectionView<TimestampedValue<UserMetadataFile>> latestFileView) {
      this.latestFileView = latestFileView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      UserMetadataFile metadata = c.sideInput(latestFileView).getValue();
      UserEvent event = c.element();
      if (metadata.userMetadata.containsKey(c.element())) {
        event.userMetadata = metadata.userMetadata.get(c.element().userId);
      } else {
        event.userMetadata = "UNSET";
      }
      c.output(event);
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Periodically trigger reading a file, and turn it into a side input.
    // We want to always keep the latest version of the file as the side
    // input value.
    PCollectionView<TimestampedValue<UserMetadataFile>> latestFileView = pipeline
        .apply("periodic generator", CountingInput.unbounded()
            .withRate(options.getElementsPerPeriod(),
                Duration.standardMinutes(options.getPeriodMinutes())))
        // Trigger the side input every time the periodic generator fires.
        .apply("Add trigger", Window.<Long>into(new GlobalWindows())
            .triggering(AfterPane.elementCountAtLeast(1))
            .discardingFiredPanes())
        // Read the latest version of the metadata file.
        .apply("Read metadata", ParDo.of(new ReadFile(options.getUserMetadataFilename())))
        // Take the latest version. This will be a noop unless the period is
        // very small, as there will be only one version of the file being processed.
        .apply("Latest", Latest.<TimestampedValue<UserMetadataFile>>globally())
        .apply("Convert to singleton", View.<TimestampedValue<UserMetadataFile>>asSingleton());

    // Read in an unbounded stream of user events.
    PCollection<UserEvent> events =
        pipeline.apply("Read user events", PubsubIO.<UserEvent>read()
            .withCoder(AvroCoder.of(UserEvent.class))
            .topic(options.getUserEventsTopic()));

    // For each event, enrich the object with metadata specific to the user.
    // Write the enriched events to the output PubSub topic.
    events.apply("Enrich", ParDo.withSideInputs(latestFileView)
        .of(new EnrichUserWithMetadata(latestFileView)))
        .apply("Write output", PubsubIO.<UserEvent>write()
            .withCoder(AvroCoder.of(UserEvent.class))
            .topic(options.getEnrichedEventsTopic()));

  }
}
