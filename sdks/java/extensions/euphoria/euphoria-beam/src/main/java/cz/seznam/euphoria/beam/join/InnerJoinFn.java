package cz.seznam.euphoria.beam.join;

import cz.seznam.euphoria.beam.SingleValueCollector;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class InnerJoinFn<LeftT, RightT, K, OutputT> extends JoinFn<LeftT, RightT, K, OutputT> {

  public InnerJoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> functor,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {
    super(functor, leftTag, rightTag);
  }

  @Override
  public void processElement(ProcessContext c) {

    KV<K, CoGbkResult> element = c.element();
    CoGbkResult value = element.getValue();
    K key = element.getKey();

    Iterable<LeftT> leftSideIter = value.getAll(leftTag);
    Iterable<RightT> rightSideIter = value.getAll(rightTag);

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
