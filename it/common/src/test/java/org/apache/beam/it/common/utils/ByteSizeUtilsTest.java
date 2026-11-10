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
package org.apache.beam.it.common.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.common.utils.ByteSizeUtils.formatBytes;
import static org.apache.beam.it.common.utils.ByteSizeUtils.parseSizeToBytes;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import org.junit.Test;

/** Unit tests for {@link ByteSizeUtils}. */
public class ByteSizeUtilsTest {

  private static final long KB = 1024L;
  private static final long MB = 1024L * KB;
  private static final long GB = 1024L * MB;
  private static final long TB = 1024L * GB;

  @Test
  public void testParseSizeToBytes() {
    assertThat(parseSizeToBytes("0")).isEqualTo(0L);
    assertThat(parseSizeToBytes("1024")).isEqualTo(1024L);
    assertThat(parseSizeToBytes("1K")).isEqualTo(KB);
    assertThat(parseSizeToBytes("4M")).isEqualTo(4 * MB);
    assertThat(parseSizeToBytes(" 40g ")).isEqualTo(40 * GB);
    assertThat(parseSizeToBytes("1T")).isEqualTo(TB);

    assertThrows(IllegalArgumentException.class, () -> parseSizeToBytes(""));
    assertThrows(IllegalArgumentException.class, () -> parseSizeToBytes("abc"));
    assertThrows(IllegalArgumentException.class, () -> parseSizeToBytes("40GB"));
  }

  @Test
  public void testFormatBytes() {
    assertThat(formatBytes(512)).isEqualTo("512 B");
    assertThat(formatBytes(40 * GB)).isEqualTo("42,949,672,960 B (40.00 GB)");
  }

  @Test
  public void testDeserializersAcceptNumbersAndSizes() throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    Options fromSizes = mapper.readValue("{\"total\":\"10G\",\"chunk\":\"24M\"}", Options.class);
    assertThat(fromSizes.total).isEqualTo(10 * GB);
    assertThat(fromSizes.chunk).isEqualTo(24 * MB);

    Options fromNumbers = mapper.readValue("{\"total\":1024,\"chunk\":2048}", Options.class);
    assertThat(fromNumbers.total).isEqualTo(1024L);
    assertThat(fromNumbers.chunk).isEqualTo(2048);
  }

  @Test
  public void testIntDeserializerRejectsOverflow() {
    ObjectMapper mapper = new ObjectMapper();
    // 4G does not fit in an int, so it must fail rather than silently wrap around.
    assertThrows(Exception.class, () -> mapper.readValue("{\"chunk\":\"4G\"}", Options.class));
  }

  /** Holder used to exercise the Jackson deserializers. */
  static class Options {
    @JsonProperty
    @JsonDeserialize(using = ByteSizeUtils.Deserializer.class)
    public long total;

    @JsonProperty
    @JsonDeserialize(using = ByteSizeUtils.IntDeserializer.class)
    public int chunk;
  }
}
