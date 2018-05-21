package org.apache.beam.sdk.extensions.euphoria.beam.join;

import org.apache.beam.sdk.extensions.euphoria.beam.SingleValueCollector;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.TupleTag;


/**
 * Left outer join implementation of {@link JoinFn}.
 */
public class LeftOuterJoinFn<LeftT, RightT, K, OutputT> extends JoinFn<LeftT, RightT, K, OutputT> {

  public LeftOuterJoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> joiner,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {
    super(joiner, leftTag, rightTag);
  }

  @Override
  protected void doJoin(
      ProcessContext c, K key, CoGbkResult value,
      Iterable<LeftT> leftSideIter,
      Iterable<RightT> rightSideIter) {

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    for (LeftT leftValue : leftSideIter) {
      if (rightSideIter.iterator().hasNext()) {
        for (RightT rightValue : rightSideIter) {
          joiner.apply(leftValue, rightValue, outCollector);
          c.output(Pair.of(key, outCollector.get()));
        }
      } else {
        joiner.apply(leftValue, null, outCollector);
        c.output(Pair.of(key, outCollector.get()));
      }
    }
  }

  @Override
  public String getFnName() {
    return "::left-outer-join";
  }

}
