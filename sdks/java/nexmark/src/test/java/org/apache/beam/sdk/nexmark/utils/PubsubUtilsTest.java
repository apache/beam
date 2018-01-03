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

package org.apache.beam.sdk.nexmark.utils;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link PubsubUtils}.
 */
public class PubsubUtilsTest {

  private static final String QUERY_NAME = "queryName";
  private static final String BASE_NAME = "baseSubscription";
  private static final String SEPARATOR = "_";
  private static final String SUFFIX = SEPARATOR + "source";
  private static final long NOW = 3929191L;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test public void testShortSubscriptionThrowsIfSubscriptionNotSpeicified() {
    thrown.expect(RuntimeException.class);
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.VERBATIM);
    options.setPubsubSubscription(null);

    PubsubUtils.shortSubscription(options, QUERY_NAME, NOW);
  }

  @Test public void testShortSubscriptionForVerbatimMode() {
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.VERBATIM);
    options.setPubsubSubscription(BASE_NAME);

    assertEquals(BASE_NAME,
        PubsubUtils.shortSubscription(options, QUERY_NAME, NOW));
  }

  @Test public void testShortSubscriptionForQueryMode() {
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.QUERY);
    options.setPubsubSubscription(BASE_NAME);

    String subscription =
        PubsubUtils.shortSubscription(options, QUERY_NAME, NOW);
    String expected = join(BASE_NAME, QUERY_NAME);

    assertEquals(expected, subscription);
  }

  @Test public void testShortSubscriptionForQueryAndSaltMode() {
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.QUERY_AND_SALT);
    options.setPubsubSubscription(BASE_NAME);

    String subscription =
        PubsubUtils.shortSubscription(options, QUERY_NAME, NOW);
    String expected = join(BASE_NAME, QUERY_NAME, String.valueOf(NOW));

    assertEquals(expected, subscription);
  }

  @Test public void testShortTopicThrowsIfSubscriptionNotSpeicified() {
    thrown.expect(RuntimeException.class);
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.VERBATIM);
    options.setPubsubTopic(null);

    PubsubUtils.shortTopic(options, QUERY_NAME, NOW);
  }

  @Test public void testShortTopicForVerbatimMode() {
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.VERBATIM);
    options.setPubsubTopic(BASE_NAME);

    assertEquals(BASE_NAME,
        PubsubUtils.shortTopic(options, QUERY_NAME, NOW));
  }

  @Test public void testShortTopicForQueryMode() {
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.QUERY);
    options.setPubsubTopic(BASE_NAME);

    String subscription =
        PubsubUtils.shortTopic(options, QUERY_NAME, NOW);
    String expected = join(BASE_NAME, QUERY_NAME);

    assertEquals(expected, subscription);
  }

  @Test public void testShortTopicForQueryAndSaltMode() {
    NexmarkOptions options = newOptions(NexmarkUtils.ResourceNameMode.QUERY_AND_SALT);
    options.setPubsubTopic(BASE_NAME);

    String subscription =
        PubsubUtils.shortTopic(options, QUERY_NAME, NOW);
    String expected = join(BASE_NAME, QUERY_NAME, String.valueOf(NOW));

    assertEquals(expected, subscription);
  }

  private static String join(String string, String anotherString) {
    return string + SEPARATOR + anotherString + SUFFIX;
  }

  private static String join(String string, String anotherString, String andAnotherString) {
    return string + SEPARATOR + anotherString + SEPARATOR + andAnotherString + SUFFIX;
  }

  private NexmarkOptions newOptions(NexmarkUtils.ResourceNameMode resourceNameMode) {
    NexmarkOptions options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    options.setResourceNameMode(resourceNameMode);
    return options;
  }
}
