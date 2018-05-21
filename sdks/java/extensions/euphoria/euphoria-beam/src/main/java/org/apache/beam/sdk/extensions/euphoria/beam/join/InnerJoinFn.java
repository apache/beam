package org.apache.beam.sdk.extensions.euphoria.beam.join;

import org.apache.beam.sdk.extensions.euphoria.beam.SingleValueCollector;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Inner join implementation of {@link JoinFn}.
 */
public class InnerJoinFn<LeftT, RightT, K, OutputT> extends JoinFn<LeftT, RightT, K, OutputT> {

  public InnerJoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> functor,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {
    super(functor, leftTag, rightTag);
  }

  @Override
  protected void doJoin(
      ProcessContext c, K key, CoGbkResult value,
      Iterable<LeftT> leftSideIter,
      Iterable<RightT> rightSideIter) {

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    for (LeftT leftItem : leftSideIter) {
      for (RightT rightItem : rightSideIter) {
        joiner.apply(leftItem, rightItem, outCollector);
        c.output(Pair.of(key, outCollector.get()));
      }
    }
  }

  @Override
  public String getFnName() {
    return "::inner-join";
  }

}
