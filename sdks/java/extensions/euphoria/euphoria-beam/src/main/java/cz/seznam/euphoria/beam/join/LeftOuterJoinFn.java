package cz.seznam.euphoria.beam.join;

import cz.seznam.euphoria.beam.SingleValueCollector;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
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
  public void processElement(ProcessContext c) {

    KV<K, CoGbkResult> element = c.element();
    CoGbkResult value = element.getValue();
    K key = element.getKey();

    Iterable<LeftT> leftSideIter = value.getAll(leftTag);
    Iterable<RightT> rightSIdeIter = value.getAll(rightTag);

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    for (LeftT leftValue : leftSideIter) {
      if (rightSIdeIter.iterator().hasNext()) {
        for (RightT rightValue : rightSIdeIter) {
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
