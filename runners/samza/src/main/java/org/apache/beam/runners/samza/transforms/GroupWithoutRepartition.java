package org.apache.beam.runners.samza.transforms;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A wrapper transform of {@link org.apache.beam.sdk.transforms.GroupByKey} or {@link
 * org.apache.beam.sdk.transforms.join.CoGroupByKey} to indicate there is no repartition needed for
 * Samza runner. For example:
 *
 * <p>input.apply(GroupWithoutRepartition.of(Count.perKey()));
 */
public class GroupWithoutRepartition<InputT extends PInput, OutputT extends POutput>
    extends PTransform<InputT, OutputT> {
  private final PTransform<InputT, OutputT> transform;

  public static <InputT extends PInput, OutputT extends POutput>
      GroupWithoutRepartition<InputT, OutputT> of(PTransform<InputT, OutputT> transform) {
    return new GroupWithoutRepartition<>(transform);
  }

  private GroupWithoutRepartition(PTransform<InputT, OutputT> transform) {
    this.transform = transform;
  }

  @Override
  @SuppressWarnings("unchecked")
  public OutputT expand(InputT input) {
    if (input instanceof PCollection) {
      return (OutputT) ((PCollection) input).apply(transform);
    } else if (input instanceof KeyedPCollectionTuple) {
      return (OutputT) ((KeyedPCollectionTuple) input).apply(transform);
    } else {
      throw new RuntimeException(
          transform.getName()
              + " is not supported with "
              + GroupWithoutRepartition.class.getSimpleName());
    }
  }
}
