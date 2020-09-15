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
package org.apache.beam.sdk.io.aws2.sns;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Assert;
import org.junit.Test;

public class SnsResponseCoderTest {

  @Test
  public void verifyResponseWithStatusCodeAndText() throws IOException {

    SnsResponse<String> expected =
        SnsResponse.create("test-1", OptionalInt.of(200), Optional.of("OK"));

    SnsResponseCoder<String> coder = SnsResponseCoder.of(StringUtf8Coder.of());
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());
    SnsResponse<String> actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void verifyResponseWithStatusAndNoText() throws IOException {
    SnsResponse<String> expected =
        SnsResponse.create("test-2", OptionalInt.of(200), Optional.empty());

    SnsResponseCoder<String> coder = SnsResponseCoder.of(StringUtf8Coder.of());
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());
    SnsResponse<String> actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void verifyResponseWithNoStatusCodeAndText() throws IOException {

    SnsResponse<String> expected =
        SnsResponse.create("test-3", OptionalInt.empty(), Optional.empty());

    SnsResponseCoder<String> coder = SnsResponseCoder.of(StringUtf8Coder.of());
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(expected, output);

    ByteArrayInputStream in = new ByteArrayInputStream(output.toByteArray());
    SnsResponse<String> actual = coder.decode(in);

    Assert.assertEquals(expected, actual);
  }
}
