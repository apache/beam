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
package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import java.util.Collections;
import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.translate.BeamFlow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

/** Unit test of {@link ListDataSource}. */
public class ListDataSourceTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test(expected = IllegalArgumentException.class)
  public void testCannotCreateInputPCollectionWhenItemsNotSerializable() {
    BeamFlow flow = BeamFlow.of(pipeline);

    DataSource<NotSerializableClass> source =
        ListDataSource.bounded(
            Collections.singletonList(new NotSerializableClass("just-some-text")));

    Dataset<NotSerializableClass> inputDataset = flow.createInput(source);

    pipeline.run();
  }

  @Test()
  public void testCanCreateInputPCollectionWhenItemsNotSerializable() {
    BeamFlow flow = BeamFlow.of(pipeline);

    DataSource<NotSerializableClass> source =
        ListDataSource.bounded(
            () ->
                Collections.singletonList(
                    Collections.singletonList(new NotSerializableClass("just-some-text"))));

    Dataset<NotSerializableClass> inputDataset = flow.createInput(source);

    PAssert.that(flow.unwrapped(inputDataset))
        .containsInAnyOrder(new NotSerializableClass("just-some-text"));

    pipeline.run();
  }

  @Test()
  public void testCanCreateInputPCollectionWhenItemsNotSerializableOnePartition() {
    BeamFlow flow = BeamFlow.of(pipeline);

    DataSource<NotSerializableClass> source =
        ListDataSource.boundedOnePartiton(
            () -> Collections.singletonList(new NotSerializableClass("just-some-text")));

    Dataset<NotSerializableClass> inputDataset = flow.createInput(source);

    PAssert.that(flow.unwrapped(inputDataset))
        .containsInAnyOrder(new NotSerializableClass("just-some-text"));

    pipeline.run();
  }

  private static class NotSerializableClass {
    private final Object someData;

    public NotSerializableClass(Object someData) {
      this.someData = someData;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      NotSerializableClass that = (NotSerializableClass) o;
      return Objects.equals(someData, that.someData);
    }

    @Override
    public int hashCode() {
      return Objects.hash(someData);
    }

    public Object getSomeData() {
      return someData;
    }
  }
}
