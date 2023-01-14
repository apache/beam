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
package org.apache.beam.sdk.io.aws2.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.io.aws2.options.SerializationTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link SSECustomerKey}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SSECustomerKeyTest {
  @Test
  public void testBuild() {
    assertThrows(
        IllegalArgumentException.class,
        () -> buildWithArgs("86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA=", null, null, null));
    assertThrows(IllegalArgumentException.class, () -> buildWithArgs(null, "AES256", null, null));
    buildWithArgs(null, null, null, null);
    buildWithArgs(
        "86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA=", "AES256", null, "SDW4BE3CQz7VqDHYKpNC5A==");
    buildWithArgs(
        "86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA=",
        "AES256",
        "SDW4BE3CQz7VqDHYKpNC5A==",
        "SDW4BE3CQz7VqDHYKpNC5A==");
  }

  public void buildWithArgs(String key, String algorithm, String md5, String encodedMD5) {
    SSECustomerKey sseCustomerKey =
        SSECustomerKey.builder().key(key).algorithm(algorithm).md5(md5).build();
    assertEquals(key, sseCustomerKey.getKey());
    assertEquals(algorithm, sseCustomerKey.getAlgorithm());
    assertEquals(encodedMD5, sseCustomerKey.getMD5());
  }

  @Test
  public void testJsonSerializeDeserialize() {
    // default key created by S3Options.SSECustomerKeyFactory
    SSECustomerKey emptyKey = SSECustomerKey.builder().build();
    assertThat(jsonSerializeDeserialize(emptyKey)).isEqualToComparingFieldByField(emptyKey);

    SSECustomerKey key = SSECustomerKey.builder().key("key").algorithm("algo").md5("md5").build();
    assertThat(jsonSerializeDeserialize(key)).isEqualToComparingFieldByField(key);
  }

  private SSECustomerKey jsonSerializeDeserialize(SSECustomerKey key) {
    return SerializationTestUtil.serializeDeserialize(SSECustomerKey.class, key);
  }
}
