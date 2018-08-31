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
