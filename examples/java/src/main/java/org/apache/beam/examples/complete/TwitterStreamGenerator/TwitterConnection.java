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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.checkerframework.checker.nullness.qual.Nullable;
import twitter4j.*;
import twitter4j.TwitterStream;
import twitter4j.conf.ConfigurationBuilder;

/** Singleton class for twitter connection* */
class TwitterConnection {
  private final BlockingQueue<Status> queue;
  private final TwitterStream twitterStream;
  private static @Nullable TwitterConnection single_instance = null;

  /**
   * Creates a new Twitter connection
   *
   * @param twitterConfig
   */
  private TwitterConnection(TwitterConfig twitterConfig) {
    this.queue = new LinkedBlockingQueue<>();
    ConfigurationBuilder cb = new ConfigurationBuilder();
    cb.setDebugEnabled(true)
        .setOAuthConsumerKey(twitterConfig.getKey())
        .setOAuthConsumerSecret(twitterConfig.getSecret())
        .setOAuthAccessToken(twitterConfig.getToken())
        .setOAuthAccessTokenSecret(twitterConfig.getTokenSecret());

    this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
    StatusListener listener =
        new StatusListener() {
          @Override
          public void onException(Exception e) {
            e.printStackTrace();
          }

          @Override
          public void onDeletionNotice(StatusDeletionNotice arg) {}

          @Override
          public void onScrubGeo(long userId, long upToStatusId) {}

          @Override
          public void onStallWarning(StallWarning warning) {}

          @Override
          public void onStatus(Status status) {
            queue.offer(status);
          }

          @Override
          public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
        };
    FilterQuery tweetFilterQuery = new FilterQuery();
    for (String filter : twitterConfig.getFilters()) {
      tweetFilterQuery.track(filter);
    }
    tweetFilterQuery.language(twitterConfig.getLanguage());
    this.twitterStream.addListener(listener);
    this.twitterStream.filter(tweetFilterQuery);
  }

  public static TwitterConnection getInstance(TwitterConfig twitterConfig) {
    if (single_instance != null) {
      return single_instance;
    }
    single_instance = new TwitterConnection(twitterConfig);
    return single_instance;
  }

  public BlockingQueue<Status> getQueue() {
    return this.queue;
  }

  public void closeStream() {
    this.twitterStream.shutdown();
  }
}
