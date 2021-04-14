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
package org.apache.beam.examples.complete.TwitterStreamGenerator;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import twitter4j.Status;

/**
 * An unbounded source for <a
 * href="https://developer.twitter.com/en/docs/tutorials/consuming-streaming-data">twitter</a>
 * stream.
 */
public class TwitterIO {

  /**
   * Initializes the stream by converting input to a Twitter connection configuration
   *
   * @param key
   * @param secret
   * @param token
   * @param tokenSecret
   * @param filters
   * @param language
   * @return
   */
  public static PTransform<PBegin, PCollection<Status>> readStandardStream(
      String key,
      String secret,
      String token,
      String tokenSecret,
      List<String> filters,
      String language,
      Long maxTweetsCount) {
    return new TwitterIO.Read.Builder()
        .setKey(key)
        .setSecret(secret)
        .setToken(token)
        .setTokenSecret(tokenSecret)
        .setFilters(filters)
        .setLanguage(language)
        .setMaxTweetsCount(maxTweetsCount)
        .build();
  }
  /** A {@link PTransform} to read from Twitter stream. usage and configuration. */
  private static class Read extends PTransform<PBegin, PCollection<Status>> {
    private final String key;
    private final String secret;
    private final String token;
    private final String tokenSecret;
    private final List<String> filters;
    private final String language;
    private final Long maxTweetsCount;

    private Read(Builder builder) {
      this.key = builder.key;
      this.secret = builder.secret;
      this.token = builder.token;
      this.tokenSecret = builder.tokenSecret;
      this.filters = builder.filters;
      this.language = builder.language;
      this.maxTweetsCount = builder.maxTweetsCount;
    }

    @Override
    public PCollection<Status> expand(PBegin input) throws IllegalArgumentException {
      if (key == null
          || secret == null
          || token == null
          || tokenSecret == null
          || filters == null
          || language == null) {
        throw new IllegalArgumentException("Please provide key, secret, token and token secret");
      }

      return input
          .apply(Create.of(new TwitterConfig(key, secret, token, tokenSecret, filters, language)))
          .apply(ParDo.of(new ReadFromTwitterDoFn(maxTweetsCount)));
    }

    private static class Builder {
      private String key = "";
      private String secret = "";
      private String token = "";
      private String tokenSecret = "";
      private List<String> filters = new ArrayList<>();
      private String language = "en";
      private Long maxTweetsCount = Long.MAX_VALUE;

      TwitterIO.Read.Builder setKey(final String key) {
        this.key = key;
        return this;
      }

      TwitterIO.Read.Builder setSecret(final String secret) {
        this.secret = secret;
        return this;
      }

      TwitterIO.Read.Builder setToken(final String token) {
        this.token = token;
        return this;
      }

      TwitterIO.Read.Builder setTokenSecret(final String tokenSecret) {
        this.tokenSecret = tokenSecret;
        return this;
      }

      TwitterIO.Read.Builder setFilters(final List<String> filters) {
        this.filters = filters;
        return this;
      }

      TwitterIO.Read.Builder setLanguage(final String language) {
        this.language = language;
        return this;
      }

      TwitterIO.Read.Builder setMaxTweetsCount(final Long maxTweetsCount) {
        this.maxTweetsCount = maxTweetsCount;
        return this;
      }

      TwitterIO.Read build() {
        return new TwitterIO.Read(this);
      }
    }
  }
}
