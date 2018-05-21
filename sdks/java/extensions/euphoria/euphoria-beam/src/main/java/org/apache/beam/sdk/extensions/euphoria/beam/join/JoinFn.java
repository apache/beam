package org.apache.beam.sdk.extensions.euphoria.beam.join;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Abstract base for joint implementations.
 *
 * @param <LeftT> type of left-side elements
 * @param <RightT> type of right-side elements
 * @param <K> key type
 * @param <OutputT> type of output elements
 */
public abstract class JoinFn<LeftT, RightT, K, OutputT> extends
    DoFn<KV<K, CoGbkResult>, Pair<K, OutputT>> {

  protected final BinaryFunctor<LeftT, RightT, OutputT> joiner;
  protected final TupleTag<LeftT> leftTag;
  protected final TupleTag<RightT> rightTag;

  protected JoinFn(
      BinaryFunctor<LeftT, RightT, OutputT> joiner,
      TupleTag<LeftT> leftTag, TupleTag<RightT> rightTag) {
    this.joiner = joiner;
    this.leftTag = leftTag;
    this.rightTag = rightTag;
  }

  @ProcessElement
  public abstract void processElement(ProcessContext c);

  public abstract String getFnName();
}
