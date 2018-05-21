package org.apache.beam.sdk.extensions.euphoria.beam.join;

import org.apache.beam.sdk.extensions.euphoria.beam.SingleValueCollector;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Right outer join implementation of {@link JoinFn}.
 */
public class RightOuterJoinFn<LeftT, RightT, K, OutputT> extends JoinFn<LeftT, RightT, K, OutputT> {

  public RightOuterJoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> joiner,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {
    super(joiner, leftTag, rightTag);
  }

  @Override
  public void processElement(ProcessContext c) {

    KV<K, CoGbkResult> element = c.element();
    CoGbkResult value = element.getValue();
    K key = element.getKey();

    Iterable<LeftT> leftSideIter = value.getAll(leftTag);
    Iterable<RightT> rightSIdeIter = value.getAll(rightTag);

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    for (RightT rightValue : rightSIdeIter) {
      if (leftSideIter.iterator().hasNext()) {
        for (LeftT leftValue : leftSideIter) {
          joiner.apply(leftValue, rightValue, outCollector);
          c.output(Pair.of(key, outCollector.get()));
        }
      } else {
        joiner.apply(null, rightValue, outCollector);
        c.output(Pair.of(key, outCollector.get()));
      }
    }

  }

  @Override
  public String getFnName() {
    return "::right-outer-join";
  }
}
