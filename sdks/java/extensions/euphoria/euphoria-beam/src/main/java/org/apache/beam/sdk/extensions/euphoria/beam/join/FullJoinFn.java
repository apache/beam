package org.apache.beam.sdk.extensions.euphoria.beam.join;

import org.apache.beam.sdk.extensions.euphoria.beam.SingleValueCollector;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
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
  public void processElement(ProcessContext c) {

    KV<K, CoGbkResult> element = c.element();
    CoGbkResult value = element.getValue();
    K key = element.getKey();

    Iterable<LeftT> leftSideIter = value.getAll(leftTag);
    Iterable<RightT> rightSIdeIter = value.getAll(rightTag);

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    boolean leftHasValues = leftSideIter.iterator().hasNext();
    boolean rightHasValues = rightSIdeIter.iterator().hasNext();

    if (leftHasValues && rightHasValues) {
      for (RightT rightValue : rightSIdeIter) {
        for (LeftT leftValue : leftSideIter) {
          joiner.apply(leftValue, rightValue, outCollector);
          c.output(Pair.of(key, outCollector.get()));
        }
      }
    } else if (leftHasValues && !rightHasValues) {
      for (LeftT leftValue : leftSideIter) {
        joiner.apply(leftValue, null, outCollector);
        c.output(Pair.of(key, outCollector.get()));
      }
    } else if (!leftHasValues && rightHasValues) {
      for (RightT rightValue : rightSIdeIter) {
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
