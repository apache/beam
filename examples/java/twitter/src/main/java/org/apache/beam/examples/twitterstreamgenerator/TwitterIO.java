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
package org.apache.beam.examples.twitterstreamgenerator;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * An unbounded source for <a
 * href="https://developer.twitter.com/en/docs/tutorials/consuming-streaming-data">twitter</a>
 * stream. PTransforms for streaming live tweets from twitter. Reading from Twitter is supported by
 * read()
 *
 * <p>Standard Twitter API can be read using a list of Twitter Config
 * readStandardStream(List<TwitterConfig>)
 *
 * <p>It allow multiple Twitter configurations to demonstrate how multiple twitter streams can be
 * combined in a single pipeline.
 *
 * <pre>{@code
 * PCollection<WeatherRecord> weatherData = pipeline.apply(
 *      TwitterIO.readStandardStream(
 *          Arrays.asList(
 *                  new TwitterConfig.Builder()
 *                      .setKey("")
 *                      .setSecret("")
 *                      .setToken("")
 *                      .setTokenSecret("")
 *                      .setFilters(Arrays.asList("", ""))
 *                      .setLanguage("en")
 *                      .setTweetsCount(10L)
 *                      .setMinutesToRun(1)
 *                      .build())));
 * }</pre>
 */
public class TwitterIO {

  /**
   * Initializes the stream by converting input to a Twitter connection configuration.
   *
   * @param twitterConfigs list of twitter config
   * @return PTransform of statuses
   */
  public static PTransform<PBegin, PCollection<String>> readStandardStream(
      List<TwitterConfig> twitterConfigs) {
    return new TwitterIO.Read.Builder().setTwitterConfig(twitterConfigs).build();
  }

  /** A {@link PTransform} to read from Twitter stream. usage and configuration. */
  private static class Read extends PTransform<PBegin, PCollection<String>> {
    private final List<TwitterConfig> twitterConfigs;

    private Read(Builder builder) {
      this.twitterConfigs = builder.twitterConfigs;
    }

    @Override
    public PCollection<String> expand(PBegin input) throws IllegalArgumentException {
      return input.apply(Create.of(this.twitterConfigs)).apply(ParDo.of(new ReadFromTwitterDoFn()));
    }

    private static class Builder {
      List<TwitterConfig> twitterConfigs = new ArrayList<>();

      TwitterIO.Read.Builder setTwitterConfig(final List<TwitterConfig> twitterConfigs) {
        this.twitterConfigs = twitterConfigs;
        return this;
      }

      TwitterIO.Read build() {
        return new TwitterIO.Read(this);
      }
    }
  }
}
