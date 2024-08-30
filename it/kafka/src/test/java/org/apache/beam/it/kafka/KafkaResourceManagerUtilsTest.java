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
package org.apache.beam.it.kafka;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link KafkaResourceManagerUtils}. */
@RunWith(JUnit4.class)
public class KafkaResourceManagerUtilsTest {
  @Test
  public void testGenerateTopicNameShouldReplaceIllegalChars() {
    String testBaseString = "^apache_beam/io\\kafka\0";
    String actual = KafkaResourceManagerUtils.generateTopicName(testBaseString);
    assertThat(actual).matches("-apache_beam-io-kafka--\\d{8}-\\d{6}-\\d{6}");
  }
}
