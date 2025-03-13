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
package org.apache.beam.sdk.schemas.io;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GenericDlqTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testDlq() {
    StoringDlqProvider.reset();
    Failure failure1 = Failure.newBuilder().setError("a").setPayload("b".getBytes(UTF_8)).build();
    Failure failure2 = Failure.newBuilder().setError("c").setPayload("d".getBytes(UTF_8)).build();
    p.apply(Create.of(failure1, failure2))
        .apply(
            GenericDlq.getDlqTransform(
                StoringDlqProvider.ID + ": " + StoringDlqProvider.CONFIG + " "));
    p.run().waitUntilFinish();
    assertThat(StoringDlqProvider.getFailures(), CoreMatchers.hasItems(failure1, failure2));
  }

  @Test
  public void testParseFailures() {
    assertThrows(
        IllegalArgumentException.class, () -> GenericDlq.getDlqTransform("no colon present"));
    assertThrows(IllegalArgumentException.class, () -> GenericDlq.getDlqTransform("bad_id:xxx"));
    assertThrows(
        IllegalArgumentException.class,
        () -> GenericDlq.getDlqTransform(StoringDlqProvider.ID + ": not config"));
  }
}
