package org.apache.beam.runners.spark.translation;

import org.apache.beam.runners.spark.coders.EncoderHelpers;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import com.google.common.base.Optional;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.Arrays;
import java.util.Collections;

import scala.Tuple2;

/**
 * Spark runner implementation of {@link Combine.KeyedCombineFn}.
 */
class SparkKeyedCombineFn<K, InputT, AccumT, OutputT>
    extends Aggregator<KV<WindowedValue<K>, InputT>, Optional<Tuple2<WindowedValue<K>, AccumT>>,
    WindowedValue<OutputT>> {

  private final Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> fn;

  public SparkKeyedCombineFn(Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> fn) {
    this.fn = fn;
  }

  @Override
  public Optional<Tuple2<WindowedValue<K>, AccumT>> zero() {
    return Optional.absent();
  }

  @Override
  public Optional<Tuple2<WindowedValue<K>, AccumT>> reduce(
      Optional<Tuple2<WindowedValue<K>, AccumT>> accumOpt, KV<WindowedValue<K>, InputT> in) {
    AccumT accum;
    WindowedValue<K> wk = in.getKey();
    if (!accumOpt.isPresent()) {
      accum = fn.createAccumulator(wk.getValue());
    } else {
      accum = accumOpt.get()._2();
    }
    accum = fn.addInput(wk.getValue(), accum, in.getValue());
    Tuple2<WindowedValue<K>, AccumT> output = new Tuple2<>(WindowedValue.of(wk.getValue(),
        wk.getTimestamp(), wk.getWindows(), wk.getPane()), accum);
    return Optional.of(output);
  }

  @Override
  public Optional<Tuple2<WindowedValue<K>, AccumT>> merge(
      Optional<Tuple2<WindowedValue<K>, AccumT>> accumOpt1,
      Optional<Tuple2<WindowedValue<K>, AccumT>> accumOpt2) {
    if (!accumOpt1.isPresent()) {
      return accumOpt2;
    } else if (!accumOpt2.isPresent()) {
      return accumOpt1;
    } else {
      WindowedValue<K> wk = accumOpt1.get()._1();
      Iterable<AccumT> accums = Collections.unmodifiableCollection(
          Arrays.asList(accumOpt1.get()._2(), accumOpt2.get()._2()));
      AccumT merged = fn.mergeAccumulators(wk.getValue(), accums);
      return Optional.of(new Tuple2<>(wk, merged));
    }
  }

  @Override
  public WindowedValue<OutputT>
  finish(Optional<Tuple2<WindowedValue<K>, AccumT>> reduction) {
    WindowedValue<K> wk = reduction.get()._1();
    AccumT accum = reduction.get()._2();
    return WindowedValue.of(fn.extractOutput(wk.getValue(), accum), wk.getTimestamp(),
        wk.getWindows(), wk.getPane());
  }

  @Override
  public Encoder<Optional<Tuple2<WindowedValue<K>, AccumT>>> bufferEncoder() {
    return EncoderHelpers.encoder();
  }

  @Override
  public Encoder<WindowedValue<OutputT>> outputEncoder() {
    return EncoderHelpers.encoder();
  }
}
