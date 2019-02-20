package org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions;

import java.util.ArrayList;
import org.apache.beam.runners.spark.structuredstreaming.translation.EncoderHelpers;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

public class AggregatorCombiner<InputT, AccumT, OutputT>
    extends Aggregator<InputT, AccumT, OutputT> {

  Combine.CombineFn<InputT, AccumT, OutputT> combineFn;
  Coder<AccumT> accumulatorCoder;
  Coder<OutputT> outputCoder;

  public AggregatorCombiner(Combine.CombineFn<InputT, AccumT, OutputT> combineFn,
      Coder<AccumT> accumulatorCoder, Coder<OutputT> outputCoder) {
    this.combineFn = combineFn;
    this.accumulatorCoder = accumulatorCoder;
    this.outputCoder = outputCoder;
  }

  @Override public AccumT zero() {
    return combineFn.createAccumulator();
  }

  @Override public AccumT reduce(AccumT accumulator, InputT input) {
    return combineFn.addInput(accumulator, input);
  }

  @Override public AccumT merge(AccumT accumulator1, AccumT accumulator2) {
    ArrayList<AccumT> accumulators = new ArrayList<>();
    accumulators.add(accumulator1);
    accumulators.add(accumulator2);
    return combineFn.mergeAccumulators(accumulators);
  }

  @Override public OutputT finish(AccumT reduction) {
    return combineFn.extractOutput(reduction);
  }

  @Override public Encoder<AccumT> bufferEncoder() {
    //TODO replace with accumulatorCoder
    return EncoderHelpers.genericEncoder();
  }

  @Override public Encoder<OutputT> outputEncoder() {
    //TODO replace with outputCoder
    return EncoderHelpers.genericEncoder();
  }
}
