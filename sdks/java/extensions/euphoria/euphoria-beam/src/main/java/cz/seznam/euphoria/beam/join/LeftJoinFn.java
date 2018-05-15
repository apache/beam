package cz.seznam.euphoria.beam.join;

import cz.seznam.euphoria.beam.SingleValueCollector;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

//TODO implement
public class LeftJoinFn<LeftT, RightT, K, OutputT> /*extends JoinFn<LeftT, RightT, K, OutputT> */{
/*
  protected LeftJoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> joiner,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {
    super(joiner, leftTag, rightTag);
  }

  @Override
  public void processElement(ProcessContext c) {

    KV<K, CoGbkResult> element = c.element();
    CoGbkResult value = element.getValue();

    Iterable<LeftT> leftSideIter = value.getAll(leftTag);
    Iterable<RightT> rightSIdeIter = value.getAll(rightTag);

    SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();


  }

  @Override
  public String getFnName() {
    return "::left-join";
  }
  */
}
