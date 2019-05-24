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
package org.apache.beam.sdk.extensions.smb.json;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link JsonBucketMetadata}. */
public class JsonBucketMetadataTest {

  @Test
  public void test() throws Exception {
    final TableRow user =
        new TableRow()
            .set("user", "Alice")
            .set("age", 10)
            .set(
                "location",
                new TableRow()
                    .set("currentCountry", "US")
                    .set("prevCountries", Arrays.asList("CN", "MX")));

    Assert.assertEquals(
        (Integer) 10,
        new JsonBucketMetadata<>(1, 1, Integer.class, HashType.MURMUR3_32, "age").extractKey(user));

    Assert.assertEquals(
        "US",
        new JsonBucketMetadata<>(1, 1, String.class, HashType.MURMUR3_32, "location.currentCountry")
            .extractKey(user));

    /*
    FIXME: BucketMetadata should allow custom coder?
    Assert.assertEquals(
        Arrays.asList("CN", "MX"),
        new JsonBucketMetadata<>(
                1, 1, ArrayList.class, HashType.MURMUR3_32, "location.prevCountries")
            .extractKey(user));
     */
  }
}
