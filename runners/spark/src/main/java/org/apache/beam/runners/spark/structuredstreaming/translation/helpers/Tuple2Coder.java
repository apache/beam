package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import scala.Tuple2;

/**
 * Beam coder to encode/decode Tuple2 scala types.
 * @param <T1> first field type parameter
 * @param <T2> second field type parameter
 */
public class Tuple2Coder<T1, T2> extends StructuredCoder<Tuple2<T1, T2>> {
  private final Coder<T1> firstFieldCoder;
  private final Coder<T2> secondFieldCoder;

  public static <K, V> Tuple2Coder<K, V> of(Coder<K> firstFieldCoder, Coder<V> secondFieldCoder) {
    return new Tuple2Coder<>(firstFieldCoder, secondFieldCoder);
  }

  private Tuple2Coder(Coder<T1> firstFieldCoder, Coder<T2> secondFieldCoder) {
    this.firstFieldCoder = firstFieldCoder;
    this.secondFieldCoder = secondFieldCoder;
  }


  @Override public void encode(Tuple2<T1, T2> value, OutputStream outStream)
      throws IOException {
    firstFieldCoder.encode(value._1(), outStream);
    secondFieldCoder.encode(value._2(), outStream);
  }

  @Override public Tuple2<T1, T2> decode(InputStream inStream) throws IOException {
    T1 firstField = firstFieldCoder.decode(inStream);
    T2 secondField = secondFieldCoder.decode(inStream);
    return Tuple2.apply(firstField, secondField);
  }

  @Override public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(firstFieldCoder, secondFieldCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic(this, "First field coder must be deterministic", firstFieldCoder);
    verifyDeterministic(this, "Second field coder must be deterministic", secondFieldCoder);
  }

  /** Returns the coder for first field. */
  public Coder<T1> getFirstFieldCoder() {
    return firstFieldCoder;
  }

  /** Returns the coder for second field. */
  public Coder<T2> getSecondFieldCoder() {
    return secondFieldCoder;
  }
}
