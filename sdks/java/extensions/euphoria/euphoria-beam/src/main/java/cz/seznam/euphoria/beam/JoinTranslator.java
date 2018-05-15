package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.beam.common.InputToKvDoFn;
import cz.seznam.euphoria.beam.io.KryoCoder;
import cz.seznam.euphoria.beam.join.InnerJoinFn;
import cz.seznam.euphoria.beam.join.JoinFn;
import cz.seznam.euphoria.beam.window.WindowingUtils;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.util.Pair;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;


/**
 * {@link OperatorTranslator Translator } for Euphoria {@link Join} operator.
 */
public class JoinTranslator implements OperatorTranslator<Join> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(Join operator, BeamExecutorContext context) {
    return doTranslate(operator, context);
  }


  public <K, LeftT, RightT, OutputT, W extends Window<W>> PCollection<Pair<K, OutputT>>
  doTranslate(Join<LeftT, RightT, K, OutputT, W> operator, BeamExecutorContext context) {

    Coder<K> keyCoder = context.getCoder(operator.getLeftKeyExtractor());

    // get input data-sets transformed to Pcollections<KV<K,LeftT/RightT>>
    List<PCollection<Object>> inputs = context.getInputs(operator);

    //TODO test left/right side indexes !
    PCollection<KV<K, LeftT>> leftKvInput = getKVInputCollection(inputs.get(0),
        operator.getLeftKeyExtractor(),
        keyCoder, new KryoCoder<>(), "::extract-keys-left");

    PCollection<KV<K, RightT>> rightKvInput = getKVInputCollection(inputs.get(1),
        operator.getRightKeyExtractor(),
        keyCoder, new KryoCoder<>(), "::extract-keys-right");

    // GoGroupByKey collections
    TupleTag<LeftT> leftTag = new TupleTag<>();
    TupleTag<RightT> rightTag = new TupleTag<>();

    PCollection<KV<K, CoGbkResult>> coGrouped = KeyedPCollectionTuple
        .of(leftTag, leftKvInput)
        .and(rightTag, rightKvInput)
        .apply("::co-group-by-key", CoGroupByKey.create());

    coGrouped = WindowingUtils
        .applyWindowingIfSpecified(operator, coGrouped, context.getAllowedLateness(operator));

    // Join
    JoinFn<LeftT, RightT, K, OutputT> joinFn = chooseJoinFn(operator, leftTag, rightTag);

    return coGrouped.apply(joinFn.getFnName(), ParDo.of(joinFn));
  }

  private <K, ValueT> PCollection<KV<K, ValueT>> getKVInputCollection(
      PCollection<Object> inputPCollection,
      UnaryFunction<ValueT, K> keyExtractor,
      Coder<K> keyCoder, Coder<ValueT> valueCoder, String transformName) {

    @SuppressWarnings("unchecked")
    PCollection<ValueT> typedInput = (PCollection<ValueT>) inputPCollection;
    typedInput.setCoder(valueCoder);

    PCollection<KV<K, ValueT>> leftKvInput =
        typedInput.apply(transformName, ParDo.of(new InputToKvDoFn<>(keyExtractor)));
    leftKvInput.setCoder(KvCoder.of(keyCoder, valueCoder));

    return leftKvInput;
  }

  private <K, LeftT, RightT, OutputT, W extends Window<W>> JoinFn<LeftT, RightT, K, OutputT>
  chooseJoinFn(
      Join<LeftT, RightT, K, OutputT, W> operator, TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag) {

    JoinFn<LeftT, RightT, K, OutputT> joinFn;
    switch (operator.getType()) {
      case INNER:
        joinFn = new InnerJoinFn<>(operator.getJoiner(), leftTag, rightTag);
        break;
      case RIGHT:
      case LEFT:
      case FULL:
      default:
        throw new UnsupportedOperationException(String.format(
            "Cannot translate Euphoria '%s' operator to Beam transformations."
                + " Given join type '%s' is not supported.",
            Join.class.getSimpleName(), operator.getType()));
    }
    return joinFn;
  }
}
