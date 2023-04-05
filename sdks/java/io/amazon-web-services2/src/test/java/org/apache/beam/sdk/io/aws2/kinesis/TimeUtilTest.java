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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TimeUtilTest {

  @Test
  public void shouldConvertToJodaInstant() {
    assertThat(TimeUtil.toJoda(null)).isNull();
    assertThat(TimeUtil.toJoda(java.time.Instant.ofEpochMilli(0L)))
        .isEqualTo(org.joda.time.Instant.ofEpochMilli(0L));
  }

  @Test
  public void shouldConvertToJavaInstant() {
    assertThat(TimeUtil.toJava(null)).isNull();
    assertThat(TimeUtil.toJava(org.joda.time.Instant.ofEpochMilli(0L)))
        .isEqualTo(java.time.Instant.ofEpochMilli(0L));
  }
}
