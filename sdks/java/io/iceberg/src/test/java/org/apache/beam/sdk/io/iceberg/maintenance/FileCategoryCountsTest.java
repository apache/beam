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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileCategoryCountsTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final FileCategoryCounts A =
      FileCategoryCounts.of(FileCategory.DATA, 3L)
          .plus(FileCategoryCounts.of(FileCategory.MANIFEST, 2L))
          .plus(FileCategoryCounts.of(FileCategory.UNKNOWN, 1L));

  private static final FileCategoryCounts B =
      FileCategoryCounts.of(FileCategory.DATA, 5L)
          .plus(FileCategoryCounts.of(FileCategory.POSITION_DELETES, 7L))
          .plus(FileCategoryCounts.of(FileCategory.STATISTICS, 11L));

  private static final FileCategoryCounts C =
      FileCategoryCounts.of(FileCategory.EQUALITY_DELETES, 13L)
          .plus(FileCategoryCounts.of(FileCategory.MANIFEST_LIST, 17L))
          .plus(FileCategoryCounts.of(FileCategory.OTHER_METADATA, 19L));

  @Test
  public void testZerosIsZeroForEveryCategory() {
    for (FileCategory category : FileCategory.values()) {
      assertEquals(0L, FileCategoryCounts.zeros().get(category));
    }
    assertEquals(0L, FileCategoryCounts.zeros().total());
  }

  @Test
  public void testOfSetsOnlyTheGivenCategory() {
    for (FileCategory category : FileCategory.values()) {
      FileCategoryCounts counts = FileCategoryCounts.of(category, 42L);
      for (FileCategory other : FileCategory.values()) {
        assertEquals(other == category ? 42L : 0L, counts.get(other));
      }
      assertEquals(42L, counts.total());
    }
  }

  @Test
  public void testPlusAddsCategoryWise() {
    FileCategoryCounts sum = A.plus(B);
    assertEquals(8L, sum.getDataFiles());
    assertEquals(7L, sum.getPositionDeleteFiles());
    assertEquals(2L, sum.getManifests());
    assertEquals(11L, sum.getStatisticsFiles());
    assertEquals(1L, sum.getUnknownFiles());
    assertEquals(A.total() + B.total(), sum.total());
  }

  @Test
  public void testPlusIsAMonoid() {
    assertEquals(A, A.plus(FileCategoryCounts.zeros()));
    assertEquals(A, FileCategoryCounts.zeros().plus(A));
    assertEquals(A.plus(B), B.plus(A));
    assertEquals(A.plus(B).plus(C), A.plus(B.plus(C)));
  }

  @Test
  public void testSchemaCoderRoundTrip() throws Exception {
    SchemaCoder<FileCategoryCounts> coder =
        SchemaRegistry.createDefault().getSchemaCoder(FileCategoryCounts.class);
    FileCategoryCounts all = A.plus(B).plus(C);
    assertEquals(all, CoderUtils.clone(coder, all));
    assertEquals(FileCategoryCounts.zeros(), CoderUtils.clone(coder, FileCategoryCounts.zeros()));
  }

  @Test
  public void testSumCombinesGlobally() {
    PCollection<FileCategoryCounts> sum =
        pipeline.apply(Create.of(A, B, C)).apply(Combine.globally(FileCategoryCounts.sum()));
    PAssert.thatSingleton(sum).isEqualTo(A.plus(B).plus(C));
    pipeline.run();
  }

  @Test
  public void testSumOfEmptyInputIsZeros() throws Exception {
    SchemaCoder<FileCategoryCounts> coder =
        pipeline.getSchemaRegistry().getSchemaCoder(FileCategoryCounts.class);
    PCollection<FileCategoryCounts> sum =
        pipeline.apply(Create.empty(coder)).apply(Combine.globally(FileCategoryCounts.sum()));
    PAssert.thatSingleton(sum).isEqualTo(FileCategoryCounts.zeros());
    pipeline.run();
  }
}
