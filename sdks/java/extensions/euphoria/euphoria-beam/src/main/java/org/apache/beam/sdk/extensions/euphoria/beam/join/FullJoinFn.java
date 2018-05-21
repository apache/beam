package org.apache.beam.sdk.extensions.euphoria.beam.join;

import org.apache.beam.sdk.extensions.euphoria.beam.SingleValueCollector;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Full join implementation of {@link JoinFn}.
 */
public class FullJoinFn<LeftT, RightT, K, OutputT> extends JoinFn<LeftT, RightT, K, OutputT> {

  public FullJoinFn(BinaryFunctor<LeftT, RightT, OutputT> joiner, TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {
    super(joiner, leftTag, rightTag);
  }

  @Override
  protected void doJoin(
      ProcessContext c, K key, CoGbkResult value,
      Iterable<LeftT> leftSideIter,
      Iterable<RightT> rightSideIter) {

    boolean leftHasValues = leftSideIter.iterator().hasNext();
    boolean rightHasValues = rightSideIter.iterator().hasNext();

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    if (leftHasValues && rightHasValues) {
      for (RightT rightValue : rightSideIter) {
        for (LeftT leftValue : leftSideIter) {
          joiner.apply(leftValue, rightValue, outCollector);
          c.output(Pair.of(key, outCollector.get()));
        }
      }
    } else if (leftHasValues) {
      for (LeftT leftValue : leftSideIter) {
        joiner.apply(leftValue, null, outCollector);
        c.output(Pair.of(key, outCollector.get()));
      }
    } else if (rightHasValues) {
      for (RightT rightValue : rightSideIter) {
        joiner.apply(null, rightValue, outCollector);
        c.output(Pair.of(key, outCollector.get()));
      }
    }
  }

  @Override
  public String getFnName() {
    return "::full-join";
  }
}
