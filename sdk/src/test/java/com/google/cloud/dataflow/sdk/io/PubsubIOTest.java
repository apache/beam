/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for PubsubIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class PubsubIOTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPubsubIOGetName() {
    assertEquals("PubsubIO.Read",
        PubsubIO.Read.topic("projects/myproject/topics/mytopic").getName());
    assertEquals("PubsubIO.Write",
        PubsubIO.Write.topic("projects/myproject/topics/mytopic").getName());
    assertEquals("ReadMyTopic",
        PubsubIO.Read.named("ReadMyTopic").topic("projects/myproject/topics/mytopic").getName());
    assertEquals("WriteMyTopic",
        PubsubIO.Write.named("WriteMyTopic").topic("projects/myproject/topics/mytopic").getName());
  }

  @Test
  public void testTopicValidationSuccess() throws Exception {
    PubsubIO.Read.topic("projects/my-project/topics/abc");
    PubsubIO.Read.topic("projects/my-project/topics/ABC");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-DeF");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-1234");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-1234-_.~%+-_.~%+-_.~%+-abc");
    PubsubIO.Read.topic(new StringBuilder().append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("11111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testTopicValidationBadCharacter() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.Read.topic("projects/my-project/topics/abc-*-abc");
  }

  @Test
  public void testTopicValidationTooLong() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.Read.topic(new StringBuilder().append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("1111111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }
}
