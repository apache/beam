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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PartitionPathsTest {
  private static final String PREFIX = "gs://lake/orders/";

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "dt", Types.StringType.get()),
          required(3, "region", Types.StringType.get()));

  @Test
  public void testRelativeIsTheFoldersAfterThePrefix() {
    assertEquals(
        "dt=2024-01-15/region=EU",
        PartitionPaths.relative(PREFIX + "dt=2024-01-15/region=EU/f.parquet", PREFIX));
    assertEquals("", PartitionPaths.relative(PREFIX + "f.parquet", PREFIX));
  }

  @Test
  public void testPartitionKeysInPath() {
    assertEquals(
        set("dt", "region"),
        PartitionPaths.partitionKeysInPath(PREFIX + "dt=2024-01-15/region=EU/f.parquet", PREFIX));
    assertEquals(
        "a value may hold '='",
        set("dt"),
        PartitionPaths.partitionKeysInPath(PREFIX + "dt=a=b/f.parquet", PREFIX));
    assertEquals(
        "plain folders and empty keys give nothing",
        set("dt"),
        PartitionPaths.partitionKeysInPath(PREFIX + "2024/=x/dt=1/f.parquet", PREFIX));
  }

  @Test
  public void testHiveNullGivesNoValue() {
    assertEquals(
        set("region"),
        PartitionPaths.partitionKeysInPath(
            PREFIX + "dt=__HIVE_DEFAULT_PARTITION__/region=EU/f.parquet", PREFIX));
  }

  @Test
  public void testNoKeysWithoutAPrefixOrOutsideIt() {
    assertEquals(set(), PartitionPaths.partitionKeysInPath(PREFIX + "dt=1/f.parquet", null));
    assertEquals(set(), PartitionPaths.partitionKeysInPath(PREFIX + "dt=1/f.parquet", ""));
    assertEquals(set(), PartitionPaths.partitionKeysInPath("gs://other/dt=1/f.parquet", PREFIX));
    assertEquals(set(), PartitionPaths.partitionKeysInPath(PREFIX + "f.parquet", PREFIX));
  }

  @Test
  public void testIdentityColumnsFilledByKeysIgnoresOtherTransforms() {
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).identity("dt").bucket("region", 4).build();
    assertEquals(
        set(2),
        PartitionPaths.identityColumnsFilledByKeys(
            spec, Arrays.asList("dt", "region_bucket", "unknown")));
    assertEquals(set(), PartitionPaths.identityColumnsFilledByKeys(spec, Collections.emptyList()));
    assertEquals(
        set(),
        PartitionPaths.identityColumnsFilledByKeys(
            PartitionSpec.unpartitioned(), Arrays.asList("dt")));
  }

  @SafeVarargs
  private static <T> HashSet<T> set(T... values) {
    return new HashSet<>(Arrays.asList(values));
  }
}
